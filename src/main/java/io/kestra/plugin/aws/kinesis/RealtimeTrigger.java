package io.kestra.plugin.aws.kinesis;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;


import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import org.reactivestreams.Publisher;

import reactor.core.publisher.FluxSink;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.core.async.SdkPublisher;

import jakarta.validation.constraints.NotNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Realtime Kinesis trigger",
            code = """
                id: realtime_kinesis
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.data }}"

                triggers:
                  - id: realtime
                    type: io.kestra.plugin.aws.kinesis.RealtimeTrigger
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: eu-central-1
                    consumerArn: "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream/consumer/kestra-app:1234abcd"
                    streamName: "stream"
                """
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Consume.ConsumedRecord> {
    @Schema(
        title = "Access Key Id in order to connect to AWS.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    private Property<String> accessKeyId;

    @Schema(
        title = "Secret Key Id in order to connect to AWS.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    private Property<String> secretKeyId;

    @Schema(
        title = "AWS session token, retrieved from an AWS token service, used for authenticating that this user has received temporary permissions to access a given resource.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    private Property<String> sessionToken;

    @Schema(
        title = "AWS region with which the SDK should communicate."
    )
    private Property<String> region;

    @Schema(
        title = "The endpoint with which the SDK should communicate.",
        description = "This property allows you to use a different S3 compatible storage backend."
    )
    private Property<String> endpointOverride;

    @Schema(
        title = "AWS STS Role.",
        description = "The Amazon Resource Name (ARN) of the role to assume. If set the task will use the `StsAssumeRoleCredentialsProvider`. If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    protected Property<String> stsRoleArn;

    @Schema(
        title = "AWS STS External Id.",
        description = " A unique identifier that might be required when you assume a role in another account. This property is only used when an `stsRoleArn` is defined."
    )
    protected Property<String> stsRoleExternalId;

    @Schema(
        title = "AWS STS Session name.",
        description = "This property is only used when an `stsRoleArn` is defined."
    )
    protected Property<String> stsRoleSessionName;

    @Schema(
        title = "The AWS STS endpoint with which the SDKClient should communicate."
    )
    protected Property<String> stsEndpointOverride;

    @Builder.Default
    @Schema(
        title = "AWS STS Session duration.",
        description = "The duration of the role session (default: 15 minutes, i.e., PT15M). This property is only used when an `stsRoleArn` is defined."
    )
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    @Schema(title = "The Kinesis stream name.")
    @NotNull
    private Property<String> streamName;

    @Builder.Default
    @Schema(
        title = "The position in the stream to start reading from.",
        description = "Kinesis iterator type: LATEST, TRIM_HORIZON, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER."
    )
    private Property<AbstractKinesis.IteratorType> iteratorType = Property.ofValue(AbstractKinesis.IteratorType.LATEST);

    @Schema(title = "Used if iteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER.")
    private Property<String> startingSequenceNumber;

    @NotNull
    private Property<String> consumerArn;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Builder.Default
    private Property<Duration> shardDiscoveryInterval = Property.ofValue(Duration.ofSeconds(30));

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) {
        RunContext runContext = conditionContext.getRunContext();

        Consume task = Consume.builder()
            .streamName(streamName)
            .region(region)
            .accessKeyId(accessKeyId)
            .secretKeyId(secretKeyId)
            .sessionToken(sessionToken)
            .endpointOverride(endpointOverride)
            .stsRoleArn(stsRoleArn)
            .stsRoleExternalId(stsRoleExternalId)
            .stsRoleSessionName(stsRoleSessionName)
            .stsEndpointOverride(stsEndpointOverride)
            .stsRoleSessionDuration(stsRoleSessionDuration)
            .build();

        return Flux.from(createFlux(task, runContext))
            .map(record -> TriggerService.generateRealtimeExecution(
                this, conditionContext, context, record
            ));
    }

    private Publisher<Consume.ConsumedRecord> createFlux(Consume task, RunContext runContext) {
        return Flux.create(sink -> {
            try (KinesisAsyncClient client = task.asyncClient(runContext)) {

                var rStreamName = runContext.render(streamName).as(String.class).orElseThrow();
                var rConsumerArn = runContext.render(consumerArn).as(String.class).orElseThrow();

                ShardManager manager = new ShardManager(client, sink, runContext, rStreamName, rConsumerArn, runContext.render(shardDiscoveryInterval).as(Duration.class).orElse(Duration.ofSeconds(30)));

                manager.start();

                waitForTermination.await();
                manager.stop();

            } catch (Exception e) {
                sink.error(e);
            } finally {
                sink.complete();
            }
        });
    }

    private class ShardManager {
        private final KinesisAsyncClient client;
        private final FluxSink<Consume.ConsumedRecord> sink;
        private final RunContext runContext;

        private final String stream;
        private final String consumerArn;
        private final Duration rediscoveryInterval;

        private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

        private final ConcurrentHashMap<String, ShardSubscriber> subscribers =
            new ConcurrentHashMap<>();

        ShardManager(KinesisAsyncClient client, FluxSink<Consume.ConsumedRecord> sink, RunContext runContext, String stream, String consumerArn, Duration rediscoveryInterval) {
            this.client = client;
            this.sink = sink;
            this.runContext = runContext;
            this.stream = stream;
            this.consumerArn = consumerArn;
            this.rediscoveryInterval = rediscoveryInterval;
        }

        void start() {
            discoverShards();

            scheduler.scheduleAtFixedRate(
                this::discoverShards,
                rediscoveryInterval.toSeconds(),
                rediscoveryInterval.toSeconds(),
                TimeUnit.SECONDS
            );
        }

        void stop() {
            scheduler.shutdownNow();
            subscribers.values().forEach(ShardSubscriber::stop);
        }

        private void discoverShards() {
            if (!isActive.get()) {
                return;
            }

            try {
                List<Shard> shards = client
                    .listShards(ListShardsRequest.builder().streamName(stream).build())
                    .get()
                    .shards();

                for (Shard shard : shards) {
                    subscribers.computeIfAbsent(shard.shardId(), sid -> {
                        try {
                            return new ShardSubscriber(client, sink, runContext,
                                stream, consumerArn, sid,
                                getInitialStartingPosition(runContext)
                            );
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).startSubscription();
                }

            } catch (Exception e) {
                sink.error(e);
            }
        }

        private StartingPosition getInitialStartingPosition(RunContext runContext) throws Exception {
            AbstractKinesis.IteratorType rIteratorType = runContext.render(iteratorType).as(AbstractKinesis.IteratorType.class).orElse(AbstractKinesis.IteratorType.LATEST);

            if (rIteratorType == AbstractKinesis.IteratorType.AT_SEQUENCE_NUMBER || rIteratorType == AbstractKinesis.IteratorType.AFTER_SEQUENCE_NUMBER) {
                return StartingPosition.builder()
                    .type(rIteratorType.name())
                    .sequenceNumber(runContext.render(startingSequenceNumber).as(String.class).orElseThrow())
                    .build();
            }

            return StartingPosition.builder()
                .type(rIteratorType.name())
                .build();
        }
    }

    private class ShardSubscriber {
        private final KinesisAsyncClient client;
        private final FluxSink<Consume.ConsumedRecord> sink;
        private final RunContext runContext;

        private final String stream;
        private final String consumerArn;

        private final String shardId;
        private volatile String lastSeq;
        private StartingPosition startingPosition;

        private final AtomicBoolean running = new AtomicBoolean(false);

        ShardSubscriber(KinesisAsyncClient client, FluxSink<Consume.ConsumedRecord> sink, RunContext runContext, String stream, String consumerArn, String shardId, StartingPosition startingPosition) {
            this.client = client;
            this.sink = sink;
            this.runContext = runContext;

            this.stream = stream;
            this.consumerArn = consumerArn;
            this.shardId = shardId;
            this.startingPosition = startingPosition;
        }

        void startSubscription() {
            if (!running.compareAndSet(false, true)) return;

            subscribeOnce();
        }

        void stop() {
            running.set(false);
        }

        private void subscribeOnce() {
            if (!running.get()) {
                return;
            }

            StartingPosition pos = (lastSeq != null) ? StartingPosition.builder()
                    .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    .sequenceNumber(lastSeq)
                    .build() : startingPosition;

            SubscribeToShardRequest req = SubscribeToShardRequest.builder()
                .consumerARN(consumerArn)
                .shardId(shardId)
                .startingPosition(pos)
                .build();

            client.subscribeToShard(req, new SubscribeToShardResponseHandler() {

                @Override
                public void responseReceived(SubscribeToShardResponse response) {
                }

                @Override
                public void onEventStream(SdkPublisher<SubscribeToShardEventStream> publisher) {
                    publisher.subscribe(evt -> {
                        if (evt instanceof SubscribeToShardEvent e) {
                            e.records().forEach(record -> {
                                lastSeq = record.sequenceNumber();

                                sink.next(
                                    Consume.ConsumedRecord.builder()
                                        .shardId(shardId)
                                        .partitionKey(record.partitionKey())
                                        .sequenceNumber(record.sequenceNumber())
                                        .data(record.data().asUtf8String())
                                        .approximateArrivalTimestamp(record.approximateArrivalTimestamp())
                                        .build()
                                );
                            });
                        }
                    });
                }

                @Override
                public void exceptionOccurred(Throwable throwable) {
                    if (running.get() && isActive.get()) {
                        resubscribe();
                    }
                }

                @Override
                public void complete() {
                    if (running.get() && isActive.get()) {
                        resubscribe();
                    }
                }
            });
        }

        private void resubscribe() {
            CompletableFuture.runAsync(() -> {
                if (!running.get()) return;
                try { Thread.sleep(250); } catch (InterruptedException ignored) {}
                subscribeOnce();
            });
        }
    }

    @Override
    public void stop() {
        stop(false);
    }

    @Override
    public void kill() {
        stop(true);
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) return;

        waitForTermination.countDown();

        if (wait) {
            try {
                waitForTermination.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }
}


package io.kestra.plugin.aws.kinesis;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;


import io.kestra.plugin.aws.AbstractConnectionInterface;
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
                    message: "{{ trigger.variables.count }}"

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

    @NotNull
    private Property<String> streamName;

    @NotNull
    @Builder.Default
    private Property<String> iteratorType = Property.ofValue("LATEST");

    private Property<String> startingSequenceNumber;

    private Property<String> accessKeyId;

    private Property<String> secretKeyId;

    private Property<String> sessionToken;

    private Property<String> region;

    private Property<String> endpointOverride;

    @NotNull
    private Property<String> consumerArn;

    protected Property<String> stsRoleArn;
    protected Property<String> stsRoleExternalId;
    protected Property<String> stsRoleSessionName;
    protected Property<String> stsEndpointOverride;
    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);


    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();

        var task = Consume.builder()
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

        return Flux.create(sink -> {
            try {
                KinesisAsyncClient client = task.asyncClient(runContext);

                var rStream = runContext.render(streamName).as(String.class).orElseThrow();
                var rConsumerArn = runContext.render(consumerArn).as(String.class).orElseThrow();

                client.listShards(ListShardsRequest.builder().streamName(rStream).build())
                    .whenComplete((shards, err) -> {
                        if (err != null) {
                            sink.error(err);
                            return;
                        }

                        for (Shard shard : shards.shards()) {
                            subscribeShard(shard, client, rConsumerArn, sink, runContext, conditionContext, context);
                        }
                    });

            } catch (Throwable t) {
                sink.error(t);
            }
        });
    }

    private void subscribeShard(Shard shard, KinesisAsyncClient client, String consumerArn, FluxSink<Execution> sink, RunContext runContext, ConditionContext conditionContext, TriggerContext context) {
        StartingPosition position;
        try {
            position = startingPosition(runContext);
        } catch (Exception e) {
            sink.error(e);
            return;
        }

        var subscribeToShardRequest = SubscribeToShardRequest.builder()
            .consumerARN(consumerArn)
            .shardId(shard.shardId())
            .startingPosition(position)
            .build();

        client.subscribeToShard(subscribeToShardRequest, new SubscribeToShardResponseHandler() {

            @Override
            public void responseReceived(SubscribeToShardResponse response) {}

            @Override
            public void onEventStream(SdkPublisher<SubscribeToShardEventStream> publisher) {
                publisher.subscribe(ev -> {
                    if (ev instanceof SubscribeToShardEvent evt) {
                        evt.records().forEach(rec -> {
                            var out = Consume.ConsumedRecord.builder()
                                .data(rec.data().asUtf8String())
                                .partitionKey(rec.partitionKey())
                                .sequenceNumber(rec.sequenceNumber())
                                .shardId(shard.shardId())
                                .approximateArrivalTimestamp(rec.approximateArrivalTimestamp())
                                .build();

                            Execution exec = TriggerService.generateRealtimeExecution(
                                RealtimeTrigger.this, conditionContext, context, out
                            );

                            sink.next(exec);
                        });
                    }
                });
            }

            @Override
            public void exceptionOccurred(Throwable t) {
                sink.error(t);
            }

            @Override
            public void complete() {
                sink.complete();
            }
        });
    }

    private StartingPosition startingPosition(RunContext runContext) throws IllegalVariableEvaluationException {
        if (startingSequenceNumber != null) {
            return StartingPosition.builder()
                .type(ShardIteratorType.AT_SEQUENCE_NUMBER)
                .sequenceNumber(runContext.render(startingSequenceNumber).as(String.class).orElseThrow())
                .build();
        }

        return StartingPosition.builder()
            .type(ShardIteratorType.fromValue(runContext.render(iteratorType).as(String.class).orElse("LATEST")))
            .build();
    }
}

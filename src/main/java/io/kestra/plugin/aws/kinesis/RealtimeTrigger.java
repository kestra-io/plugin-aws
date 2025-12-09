package io.kestra.plugin.aws.kinesis;

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
                  - id: print
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.value.data }}"

                triggers:
                  - id: realtime
                    type: io.kestra.plugin.aws.kinesis.KinesisRealtimeTrigger
                    streamName: "my-stream"
                """
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Consume.ConsumedRecord> {

    @NotNull
    private Property<String> stream;

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
            .stream(stream)
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

        KinesisAsyncClient client = task.asyncClient(runContext);

        return Flux.create(sink -> {
            try {
                ListShardsResponse shards = client.listShards(ListShardsRequest.builder().streamName(runContext.render(stream).as(String.class).orElseThrow()).build()).get();

                for (Shard shard : shards.shards()) {
                    var shardId = shard.shardId();

                    SubscribeToShardRequest subscribeToShardRequest = SubscribeToShardRequest.builder()
                        .consumerARN(runContext.render(consumerArn).as(String.class).orElseThrow())
                        .shardId(shardId)
                        .startingPosition(startingPosition(runContext))
                        .build();

                    client.subscribeToShard(
                        subscribeToShardRequest,
                        new SubscribeToShardResponseHandler() {
                            @Override
                            public void responseReceived(SubscribeToShardResponse r) {
                            }

                            @Override
                            public void onEventStream(SdkPublisher<SubscribeToShardEventStream> publisher) {
                                publisher.subscribe(event -> {
                                    if (event instanceof SubscribeToShardEvent recordsEvent) {
                                        recordsEvent.records().forEach(rec -> {
                                            Consume.ConsumedRecord consumedRecord = Consume.ConsumedRecord.builder()
                                                .data(rec.data().asUtf8String())
                                                .partitionKey(rec.partitionKey())
                                                .sequenceNumber(rec.sequenceNumber())
                                                .shardId(shardId)
                                                .approximateArrivalTimestamp(rec.approximateArrivalTimestamp())
                                                .build();

                                            Execution execution = TriggerService.generateRealtimeExecution(RealtimeTrigger.this, conditionContext, context, consumedRecord);

                                            sink.next(execution);
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
                        }
                    );
                }
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    private StartingPosition startingPosition(RunContext runContext) throws Exception {
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

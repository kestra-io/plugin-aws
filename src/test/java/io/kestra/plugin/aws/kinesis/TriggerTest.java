package io.kestra.plugin.aws.kinesis;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.*;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.aws.kinesis.model.Record;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class TriggerTest extends AbstractKinesisTest {
    @Test
    void evaluate() throws Exception {
        Record record = Record.builder()
            .partitionKey("pk")
            .data("record")
            .build();

        Record record2 = Record.builder()
            .partitionKey("pk")
            .data("record 2")
            .build();

        Record record3 = Record.builder()
            .partitionKey("pk")
            .data("record 3")
            .build();

        var put = PutRecords.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .streamName(Property.ofValue(streamName))
            .records(List.of(record, record2, record3))
            .build();

        put.run(runContextFactory.of());

        var trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getName())
            .streamName(Property.ofValue(streamName))
            .iteratorType(Property.ofValue(AbstractKinesis.IteratorType.TRIM_HORIZON))
            .maxRecords(Property.ofValue(3))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        PutRecords.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .streamName(Property.ofValue(streamName))
            .records(List.of(Record.builder().partitionKey("pk").data("record 4").build()))
            .build()
            .run(runContextFactory.of());

        var nextExecution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(nextExecution.isPresent(), is(true));
        assertThat(nextExecution.orElseThrow().getTrigger().getVariables().get("count"), is(1));
    }

    @Test
    void evaluateLatest() throws Exception {
        var trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + "Latest")
            .type(TriggerTest.class.getName())
            .streamName(Property.ofValue(streamName))
            .iteratorType(Property.ofValue(AbstractKinesis.IteratorType.LATEST))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        PutRecords.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .streamName(Property.ofValue(streamName))
            .records(List.of(Record.builder().partitionKey("pk").data("latest record").build()))
            .build()
            .run(runContextFactory.of());

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));
    }

    @Test
    void evaluateTwoShardsTrimHorizon() throws Exception {
        var multiShardStream = "stream-2shards-" + IdUtils.create();

        try (
            KinesisClient kinesisClient = KinesisClient.builder()
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
                    )
                )
                .region(Region.of(REGION))
                .endpointOverride(URI.create(endpointUrl()))
                .build()
        ) {
            kinesisClient.createStream(
                CreateStreamRequest.builder()
                    .streamName(multiShardStream)
                    .shardCount(2)
                    .build()
            );

            DescribeStreamResponse ds;
            do {
                Thread.sleep(200);
                ds = kinesisClient.describeStream(r -> r.streamName(multiShardStream));
            } while (ds.streamDescription().streamStatus() != StreamStatus.ACTIVE);
        }

        PutRecords.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .streamName(Property.ofValue(multiShardStream))
            .records(List.of(
                Record.builder().partitionKey("pk-1").data("record 1").build(),
                Record.builder().partitionKey("pk-2").data("record 2").build()
            ))
            .build()
            .run(runContextFactory.of());

        var trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + "TwoShards")
            .type(TriggerTest.class.getName())
            .streamName(Property.ofValue(multiShardStream))
            .iteratorType(Property.ofValue(AbstractKinesis.IteratorType.TRIM_HORIZON))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        assertThat(execution.orElseThrow().getTrigger().getVariables().get("count"), is(2));

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        PutRecords.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .streamName(Property.ofValue(multiShardStream))
            .records(List.of(Record.builder().partitionKey("pk-1").data("record 3").build()))
            .build()
            .run(runContextFactory.of());

        var nextExecution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(nextExecution.isPresent(), is(true));
        assertThat(nextExecution.orElseThrow().getTrigger().getVariables().get("count"), is(1));
    }

    @Test
    void evaluateFallbackOnInvalidSequenceNumber() throws Exception {
        PutRecords.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .streamName(Property.ofValue(streamName))
            .records(List.of(Record.builder().partitionKey("pk").data("fallback record").build()))
            .build()
            .run(runContextFactory.of());

        var stateKey = "kinesis_fallback_test";
        var trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName() + "Fallback")
            .type(TriggerTest.class.getName())
            .streamName(Property.ofValue(streamName))
            .iteratorType(Property.ofValue(AbstractKinesis.IteratorType.TRIM_HORIZON))
            .stateKey(Property.ofValue(stateKey))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var streamScope = REGION.length() + "_" + REGION + endpointUrl().length() + "_" + endpointUrl() + streamName.length() + "_" + streamName;
        var invalidState = new HashMap<String, StatefulTriggerService.Entry>();
        invalidState.put(streamScope + "shardId-000000000000", StatefulTriggerService.Entry.candidate(
            streamScope + "shardId-000000000000",
            "99999999999999999999999999999999999999999999999999999999",
            Instant.now()
        ));
        StatefulTriggerService.writeState(context.getKey().getRunContext(), stateKey, invalidState, Optional.empty());

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));
        assertThat(execution.orElseThrow().getTrigger().getVariables().get("count"), is(1));
    }
}

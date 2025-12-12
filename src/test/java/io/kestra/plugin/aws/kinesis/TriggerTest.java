package io.kestra.plugin.aws.kinesis;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.kestra.plugin.aws.kinesis.model.Record;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
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
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
    }
}

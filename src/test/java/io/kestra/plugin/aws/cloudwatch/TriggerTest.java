package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.kestra.core.models.property.Property;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class TriggerTest extends AbstractLocalStackTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void evaluate() throws Exception {
        RunContext runContext = runContextFactory.of();

        var push = Push.builder()
            .id(IdUtils.create())
            .type(TriggerTest.class.getSimpleName())
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .namespace(Property.ofValue("Custom/Test"))
            .metrics(Property.ofValue(List.of(
                Push.MetricValue.builder()
                    .metricName(Property.ofValue("TriggerLatency"))
                    .value(Property.ofValue(456.7))
                    .unit(Property.ofValue("Milliseconds"))
                    .dimensions(Property.ofValue(Map.of("env", "test")))
                    .build()
            )))
            .build();

        Push.Output pushOutput = push.run(runContext);
        assertThat(pushOutput.getCount(), equalTo(1));

        var trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(TriggerTest.class.getSimpleName())
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .namespace(Property.ofValue("Custom/Test"))
            .metricName(Property.ofValue("TriggerLatency"))
            .statistic(Property.ofValue("Average"))
            .periodSeconds(Property.ofValue(60))
            .window(Property.ofValue(Duration.ofMinutes(5)))
            .dimensions(Property.ofValue(List.of(
                Query.DimensionKV.builder()
                    .name(Property.ofValue("env"))
                    .value(Property.ofValue("test"))
                    .build()
            )))
            .build();

        var conditionContext = io.kestra.core.utils.TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = trigger.evaluate(conditionContext.getKey(), conditionContext.getValue());

        assertThat(execution.isPresent(), is(true));
    }
}

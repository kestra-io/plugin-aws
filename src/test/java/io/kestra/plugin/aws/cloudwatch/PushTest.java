package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class PushTest extends AbstractLocalStackTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void runPushMetric() throws Exception {
        RunContext runContext = runContextFactory.of();

        var push = Push.builder()
            .id(IdUtils.create())
            .type(PushTest.class.getSimpleName())
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .namespace(Property.ofValue("Custom/Test"))
            .metrics(Property.ofValue(List.of(
                Push.MetricValue.builder()
                    .metricName(Property.ofValue("RequestCount"))
                    .value(Property.ofValue(10.0))
                    .unit(Property.ofValue("Count"))
                    .dimensions(Property.ofValue(Map.of("env", "dev")))
                    .build()
            )))
            .build();

        Push.Output output = push.run(runContext);

        assertThat(output.getCount(), equalTo(1));
    }
}

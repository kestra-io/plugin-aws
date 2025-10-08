package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Testcontainers
class QueryTest extends AbstractLocalStackTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void runQueryMetric() throws Exception {
        RunContext runContext = runContextFactory.of();

        var push = Push.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .namespace(Property.ofValue("Custom/Test"))
            .metrics(Property.ofValue(List.of(
                Push.MetricValue.builder()
                    .metricName(Property.ofValue("LatencyMs"))
                    .value(Property.ofValue(123.45))
                    .unit(Property.ofValue("Milliseconds"))
                    .dimensions(Property.ofValue(Map.of("env", "dev")))
                    .build()
            )))
            .build();

        push.run(runContext);

        var query = Query.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .namespace(Property.ofValue("Custom/Test"))
            .metricName(Property.ofValue("LatencyMs"))
            .statistic(Property.ofValue("Average"))
            .periodSeconds(Property.ofValue(60))
            .window(Property.ofValue(Duration.ofMinutes(5)))
            .dimensions(Property.ofValue(List.of(
                Query.DimensionKV.builder()
                    .name(Property.ofValue("env"))
                    .value(Property.ofValue("dev"))
                    .build()
            )))
            .build();

        Query.Output out = query.run(runContext);

        assertThat(out.getSeries(), notNullValue());
        assertThat(out.getCount(), greaterThanOrEqualTo(0));
    }
}

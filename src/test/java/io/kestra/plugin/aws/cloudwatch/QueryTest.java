package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Datapoint;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsResponse;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class QueryTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void runQueryMetric() throws Exception {
        var runContext = runContextFactory.of();
        var client = mock(CloudWatchClient.class);

        var oldTimestamp = Instant.parse("2026-02-12T16:00:00Z");
        var newTimestamp = Instant.parse("2026-02-12T16:01:00Z");
        when(client.getMetricStatistics(any(GetMetricStatisticsRequest.class))).thenReturn(GetMetricStatisticsResponse.builder()
            .datapoints(
                Datapoint.builder().timestamp(newTimestamp).average(22.0).build(),
                Datapoint.builder().timestamp(oldTimestamp).average(11.0).build()
            )
            .build());

        var query = spy(Query.builder()
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
            .build());

        doReturn(client).when(query).client(any());

        var out = query.run(runContext);

        assertThat(out.getCount(), is(2));
        assertThat(out.getSeries(), hasSize(2));
        assertThat(out.getSeries().getFirst().values(), hasItem(oldTimestamp.toString()));
        assertThat(out.getSeries().get(1).values(), hasItem(newTimestamp.toString()));
        assertThat(out.getSeries().getFirst().values(), hasItem(11.0));
        assertThat(out.getSeries().get(1).values(), hasItem(22.0));
        verify(client, times(1)).getMetricStatistics(any(GetMetricStatisticsRequest.class));
    }
}

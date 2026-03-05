package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class PushTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void runPushMetric() throws Exception {
        var runContext = runContextFactory.of();
        var client = mock(CloudWatchClient.class);
        when(client.putMetricData(any(PutMetricDataRequest.class))).thenReturn(PutMetricDataResponse.builder().build());

        var push = spy(Push.builder()
            .id(IdUtils.create())
            .type(PushTest.class.getSimpleName())
            .namespace(Property.ofValue("Custom/Test"))
            .metrics(Property.ofValue(List.of(
                Push.MetricValue.builder()
                    .metricName(Property.ofValue("RequestCount"))
                    .value(Property.ofValue(10.0))
                    .unit(Property.ofValue("Count"))
                    .dimensions(Property.ofValue(Map.of("env", "dev")))
                    .build()
            )))
            .build());

        doReturn(client).when(push).client(any());

        var output = push.run(runContext);

        assertThat(output.getCount(), equalTo(1));
        verify(client, times(1)).putMetricData(any(PutMetricDataRequest.class));
    }
}

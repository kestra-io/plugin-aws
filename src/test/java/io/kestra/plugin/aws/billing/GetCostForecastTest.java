package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.costexplorer.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class GetCostForecastTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenForecast_whenRun_thenReturnsTotalAndPeriods() throws Exception {
        var task = GetCostForecast.builder()
            .id("forecast")
            .type(GetCostForecast.class.getName())
            .region(Property.ofValue("us-east-1"))
            .startDate(Property.ofValue("2026-10-14"))
            .endDate(Property.ofValue("2026-11-01"))
            .metric(Property.ofValue(software.amazon.awssdk.services.costexplorer.model.Metric.UNBLENDED_COST))
            .granularity(Property.ofValue(Granularity.MONTHLY))
            .predictionIntervalLevel(Property.ofValue(80))
            .build();

        var client = mock(CostExplorerClient.class);
        when(client.getCostForecast(any(GetCostForecastRequest.class))).thenReturn(
            GetCostForecastResponse.builder()
                .total(MetricValue.builder().amount("123.45").unit("USD").build())
                .forecastResultsByTime(
                    ForecastResult.builder()
                        .timePeriod(DateInterval.builder().start("2026-10-14").end("2026-11-01").build())
                        .meanValue("123.45")
                        .predictionIntervalLowerBound("100")
                        .predictionIntervalUpperBound("150")
                        .build()
                )
                .build()
        );
        var spy = spy(task);
        doReturn(client).when(spy).client(any(RunContext.class));

        var output = spy.run(runContextFactory.of());

        assertThat(output.getTotalAmount(), is(new BigDecimal("123.45")));
        assertThat(output.getUnit(), is("USD"));
        assertThat(output.getForecast(), hasSize(1));
        assertThat(output.getForecast().getFirst().get("upperBound"), is(new BigDecimal("150")));

        var captor = ArgumentCaptor.forClass(GetCostForecastRequest.class);
        verify(client).getCostForecast(captor.capture());
        assertThat(captor.getValue().metric(), is(software.amazon.awssdk.services.costexplorer.model.Metric.UNBLENDED_COST));
        assertThat(captor.getValue().predictionIntervalLevel(), is(80));
    }
}

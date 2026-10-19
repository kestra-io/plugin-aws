package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.costexplorer.model.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Forecast AWS spend with Cost Explorer",
    description = """
        Calls the Cost Explorer `GetCostForecast` API for a future time period and returns the forecasted total and the
        forecast for each period. The start date must be today or later. This is a single paid request ($0.01)."""
)
@Plugin(
    examples = {
        @Example(
            title = "Forecast the unblended cost until the end of the current month.",
            full = true,
            code = """
                id: aws_cost_forecast
                namespace: company.finops

                tasks:
                  - id: forecast
                    type: io.kestra.plugin.aws.billing.GetCostForecast
                    startDate: "{{ now() | date('yyyy-MM-dd') }}"
                    endDate: "{{ now() | dateAdd(1, 'MONTHS') | date('yyyy-MM-01') }}"
                    granularity: MONTHLY
                    metric: UNBLENDED_COST
                    predictionIntervalLevel: 80

                  - id: log_forecast
                    type: io.kestra.plugin.core.log.Log
                    message: "Forecast for the rest of the month: {{ outputs.forecast.totalAmount }} {{ outputs.forecast.unit }}"

                pluginDefaults:
                  - type: io.kestra.plugin.aws
                    values:
                      region: us-east-1
                      accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                      secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                """
        )
    },
    metrics = {
        @Metric(name = "requests", type = Counter.TYPE, unit = "requests", description = "Number of paid Cost Explorer requests made.")
    }
)
public class GetCostForecast extends AbstractCostExplorer implements RunnableTask<GetCostForecast.Output> {
    @Schema(
        title = "Metric",
        description = "The cost metric to forecast, for example `UNBLENDED_COST`, `AMORTIZED_COST`, `NET_UNBLENDED_COST` or `BLENDED_COST`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<software.amazon.awssdk.services.costexplorer.model.Metric> metric;

    @Schema(title = "Granularity", description = "`DAILY` or `MONTHLY`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<Granularity> granularity;

    @Schema(
        title = "Prediction interval level",
        description = "Confidence level of the prediction interval, between 51 and 99. When set, each forecast entry also has `lowerBound` and `upperBound`."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> predictionIntervalLevel;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var requestBuilder = GetCostForecastRequest.builder()
            .timePeriod(dateInterval(runContext))
            .metric(BillingFetch.required(runContext, metric, "metric", software.amazon.awssdk.services.costexplorer.model.Metric.class))
            .granularity(BillingFetch.required(runContext, granularity, "granularity", Granularity.class));
        var rPredictionIntervalLevel = runContext.render(predictionIntervalLevel).as(Integer.class);
        if (rPredictionIntervalLevel.isPresent()) {
            var level = rPredictionIntervalLevel.get();
            if (level < 51 || level > 99) {
                throw new IllegalArgumentException("predictionIntervalLevel must be between 51 and 99, got " + level);
            }
            requestBuilder.predictionIntervalLevel(level);
        }

        GetCostForecastResponse response;
        try (var client = client(runContext)) {
            response = client.getCostForecast(requestBuilder.build());
        }
        runContext.metric(Counter.of("requests", 1));

        var forecast = response.forecastResultsByTime().stream()
            .map(result ->
            {
                var entry = new LinkedHashMap<String, Object>();
                entry.put("start", result.timePeriod().start());
                entry.put("end", result.timePeriod().end());
                entry.put("meanValue", decimal(result.meanValue()));
                entry.put("lowerBound", decimal(result.predictionIntervalLowerBound()));
                entry.put("upperBound", decimal(result.predictionIntervalUpperBound()));
                return (Map<String, Object>) entry;
            })
            .toList();

        var total = response.total();
        return Output.builder()
            .totalAmount(total == null ? null : decimal(total.amount()))
            .unit(total == null ? null : total.unit())
            .forecast(forecast)
            .build();
    }

    private static BigDecimal decimal(String value) {
        return value == null ? null : new BigDecimal(value);
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Forecasted total", description = "Total forecasted amount for the whole time period.")
        private BigDecimal totalAmount;

        @Schema(title = "Cost unit", description = "Currency of the forecast, usually `USD`.")
        private String unit;

        @Schema(
            title = "Forecast by period",
            description = "One entry per period with `start`, `end`, `meanValue`, and `lowerBound` / `upperBound` when `predictionIntervalLevel` is set."
        )
        private List<Map<String, Object>> forecast;
    }
}

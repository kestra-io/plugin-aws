package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
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
import io.kestra.core.models.tasks.common.FetchType;
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
    title = "List AWS cost anomalies detected by Cost Anomaly Detection",
    description = """
        Calls the Cost Explorer `GetAnomalies` API to list the cost anomalies detected in a time period, optionally for
        one anomaly monitor and above a minimum total impact. Unlike the other Cost Explorer tasks, `endDate` is inclusive here,
        so `startDate` and `endDate` can be the same day. Each paginated request costs $0.01."""
)
@Plugin(
    examples = {
        @Example(
            title = "List anomalies of the last 7 days with an impact of at least 100 USD and log how many were found.",
            full = true,
            code = """
                id: aws_cost_anomalies
                namespace: company.finops

                tasks:
                  - id: anomalies
                    type: io.kestra.plugin.aws.billing.GetAnomalies
                    startDate: "{{ now() | dateAdd(-7, 'DAYS') | date('yyyy-MM-dd') }}"
                    endDate: "{{ now() | date('yyyy-MM-dd') }}"
                    minImpactUsd: 100
                    fetchType: FETCH

                  - id: log_anomalies
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.anomalies.size }} anomalies, {{ outputs.anomalies.totalImpact }} USD of total impact"

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
        @Metric(name = "requests", type = Counter.TYPE, unit = "requests", description = "Number of paid Cost Explorer requests made."),
        @Metric(name = "fetch.rows", type = Counter.TYPE, unit = "rows", description = "Number of anomalies fetched or stored.")
    }
)
public class GetAnomalies extends AbstractCostExplorer implements RunnableTask<GetAnomalies.Output> {
    @Schema(
        title = "Monitor ARN",
        description = "Only return anomalies detected by this anomaly monitor. All monitors when not set."
    )
    @PluginProperty(group = "processing")
    private Property<String> monitorArn;

    @Schema(
        title = "Minimum total impact (USD)",
        description = "Only return anomalies whose total impact is greater than or equal to this amount."
    )
    @PluginProperty(group = "processing")
    private Property<Double> minImpactUsd;

    @Schema(
        title = "Maximum number of pages",
        description = "Stops after this many `GetAnomalies` requests, each one costing $0.01, and logs a warning if more anomalies were available."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> maxPages = Property.ofValue(BillingFetch.DEFAULT_MAX_PAGES);

    @Schema(
        title = "Fetch strategy",
        description = "Default STORE writes all anomalies to internal storage. FETCH returns all anomalies, FETCH_ONE the first one, NONE only the count and total impact."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var start = parseDate("startDate", runContext.render(startDate).as(String.class).orElseThrow());
        var end = parseDate("endDate", runContext.render(endDate).as(String.class).orElseThrow());
        if (end.isBefore(start)) {
            throw new IllegalArgumentException("endDate (" + end + ") must not be before startDate (" + start + ")");
        }
        var requestBuilder = GetAnomaliesRequest.builder()
            .dateInterval(AnomalyDateInterval.builder().startDate(start.toString()).endDate(end.toString()).build());
        runContext.render(monitorArn).as(String.class).ifPresent(requestBuilder::monitorArn);
        runContext.render(minImpactUsd).as(Double.class).ifPresent(
            min -> requestBuilder.totalImpact(
                TotalImpactFilter.builder().numericOperator(NumericOperator.GREATER_THAN_OR_EQUAL).startValue(min).build()
            )
        );
        int rMaxPages = BillingFetch.maxPages(runContext, maxPages);

        var rows = new ArrayList<Map<String, Object>>();
        var totalImpact = BigDecimal.ZERO;
        String nextPageToken = null;
        int pages = 0;

        try (var client = client(runContext)) {
            do {
                var response = client.getAnomalies(requestBuilder.nextPageToken(nextPageToken).build());
                pages++;
                for (var anomaly : response.anomalies()) {
                    rows.add(toMap(anomaly));
                    if (anomaly.impact() != null && anomaly.impact().totalImpact() != null) {
                        totalImpact = totalImpact.add(BigDecimal.valueOf(anomaly.impact().totalImpact()));
                    }
                }
                nextPageToken = response.nextPageToken();
            } while (nextPageToken != null && pages < rMaxPages);
        }

        runContext.metric(Counter.of("requests", pages));
        if (nextPageToken != null) {
            runContext.logger().warn("Stopped after maxPages={} Cost Explorer requests, more anomalies are available. Increase maxPages or narrow the request.", rMaxPages);
        }

        var fetched = BillingFetch.apply(runContext, runContext.render(fetchType).as(FetchType.class).orElse(FetchType.STORE), rows);
        return Output.builder()
            .rows(fetched.rows())
            .row(fetched.row())
            .uri(fetched.uri())
            .size(fetched.size())
            .totalImpact(totalImpact)
            .truncated(nextPageToken != null)
            .build();
    }

    static Map<String, Object> toMap(Anomaly anomaly) {
        var map = new LinkedHashMap<String, Object>();
        map.put("anomalyId", anomaly.anomalyId());
        map.put("anomalyStartDate", anomaly.anomalyStartDate());
        map.put("anomalyEndDate", anomaly.anomalyEndDate());
        map.put("dimensionValue", anomaly.dimensionValue());
        map.put("monitorArn", anomaly.monitorArn());
        if (anomaly.impact() != null) {
            map.put("totalImpact", anomaly.impact().totalImpact());
            map.put("totalImpactPercentage", anomaly.impact().totalImpactPercentage());
            map.put("maxImpact", anomaly.impact().maxImpact());
            map.put("totalActualSpend", anomaly.impact().totalActualSpend());
            map.put("totalExpectedSpend", anomaly.impact().totalExpectedSpend());
        }
        if (anomaly.anomalyScore() != null) {
            map.put("maxScore", anomaly.anomalyScore().maxScore());
            map.put("currentScore", anomaly.anomalyScore().currentScore());
        }
        map.put("feedback", anomaly.feedbackAsString());
        map.put("rootCauses", anomaly.rootCauses().stream().map(cause ->
        {
            var rootCause = new LinkedHashMap<String, Object>();
            rootCause.put("service", cause.service());
            rootCause.put("region", cause.region());
            rootCause.put("usageType", cause.usageType());
            rootCause.put("linkedAccount", cause.linkedAccount());
            rootCause.put("linkedAccountName", cause.linkedAccountName());
            return rootCause;
        }).toList());
        return map;
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Anomalies",
            description = "All anomalies when fetchType is FETCH, each with `anomalyId`, dates, `dimensionValue`, `monitorArn`, " +
                "impact amounts in USD (`totalImpact`, `maxImpact`, `totalActualSpend`, `totalExpectedSpend`), scores and `rootCauses`."
        )
        private List<Map<String, Object>> rows;

        @Schema(title = "First anomaly", description = "Single anomaly when fetchType is FETCH_ONE.")
        private Map<String, Object> row;

        @Schema(title = "Stored data URI", description = "Internal storage URI of an ION file with all anomalies when fetchType is STORE.")
        private URI uri;

        @Schema(title = "Anomaly count", description = "Number of anomalies returned.")
        private Long size;

        @Schema(title = "Total impact (USD)", description = "Sum of the total impact of all returned anomalies.")
        private BigDecimal totalImpact;

        @Schema(title = "Truncated", description = "True when `maxPages` was reached before all anomalies were read.")
        private Boolean truncated;
    }
}

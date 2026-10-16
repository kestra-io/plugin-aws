package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    title = "Get AWS cost and usage from Cost Explorer",
    description = """
        Calls the Cost Explorer `GetCostAndUsage` API for a time period and returns one row per time period and group.
        Cost Explorer is a paid API: AWS charges $0.01 for each paginated request, so request only the metrics,
        granularity and groups you need, and keep `maxPages` low. `HOURLY` granularity must be enabled on the account
        and is charged separately."""
)
@Plugin(
    examples = {
        @Example(
            title = "Get last month's unblended cost per AWS service and log the total.",
            full = true,
            code = """
                id: aws_monthly_cost_by_service
                namespace: company.finops

                tasks:
                  - id: cost
                    type: io.kestra.plugin.aws.billing.GetCostAndUsage
                    startDate: "{{ now() | dateAdd(-1, 'MONTHS') | date('yyyy-MM-01') }}"
                    endDate: "{{ now() | date('yyyy-MM-01') }}"
                    granularity: MONTHLY
                    metrics:
                      - UnblendedCost
                    groupBy:
                      - SERVICE
                    fetchType: FETCH

                  - id: log_total
                    type: io.kestra.plugin.core.log.Log
                    message: "Last month: {{ outputs.cost.costTotals.UnblendedCost }} {{ outputs.cost.unit }} over {{ outputs.cost.size }} services"

                pluginDefaults:
                  - type: io.kestra.plugin.aws
                    values:
                      region: us-east-1
                      accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                      secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                """
        ),
        @Example(
            title = "Store daily cost of the last 7 days for one team tag, grouped by usage type.",
            full = true,
            code = """
                id: aws_daily_cost_team
                namespace: company.finops

                tasks:
                  - id: cost
                    type: io.kestra.plugin.aws.billing.GetCostAndUsage
                    startDate: "{{ now() | dateAdd(-7, 'DAYS') | date('yyyy-MM-dd') }}"
                    endDate: "{{ now() | date('yyyy-MM-dd') }}"
                    granularity: DAILY
                    metrics:
                      - UnblendedCost
                      - UsageQuantity
                    groupBy:
                      - USAGE_TYPE
                    dimensionFilter:
                      "TAG:team":
                        - data-platform

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
        @Metric(name = "fetch.rows", type = Counter.TYPE, unit = "rows", description = "Number of rows fetched or stored.")
    }
)
public class GetCostAndUsage extends AbstractCostExplorer implements RunnableTask<GetCostAndUsage.Output> {
    private static final int MAX_GROUP_BY = 2;

    private static final Set<String> USAGE_METRICS = Set.of("UsageQuantity", "NormalizedUsageAmount");

    @Schema(
        title = "Granularity",
        description = "`DAILY`, `MONTHLY` or `HOURLY`. `HOURLY` must be enabled on the account."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Granularity> granularity;

    @Schema(
        title = "Metrics",
        description = "Cost Explorer metrics to return, for example `UnblendedCost`, `AmortizedCost`, `NetUnblendedCost`, " +
            "`BlendedCost` or `UsageQuantity`. At least one is required."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> metrics;

    @Schema(
        title = "Group by",
        description = "Up to 2 groupings. A plain value is a dimension (e.g. `SERVICE`, `LINKED_ACCOUNT`, `USAGE_TYPE`, `REGION`), " +
            "`TAG:<key>` groups by a cost allocation tag and `COST_CATEGORY:<name>` by a cost category."
    )
    @PluginProperty(group = "processing")
    private Property<List<String>> groupBy;

    @Schema(
        title = "Filter",
        description = "Only include costs matching all entries. Keys use the same syntax as `groupBy` " +
            "(`SERVICE`, `TAG:team`, `COST_CATEGORY:env`), values are the accepted values for that key."
    )
    @PluginProperty(group = "processing")
    private Property<Map<String, List<String>>> dimensionFilter;

    @Schema(
        title = "Maximum number of pages",
        description = "Stops after this many `GetCostAndUsage` requests, each one costing $0.01, and logs a warning if more data was available."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> maxPages = Property.ofValue(10);

    @Schema(
        title = "Fetch strategy",
        description = "Default STORE writes all rows to internal storage. FETCH returns all rows, FETCH_ONE the first row, NONE only the totals."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rMetrics = runContext.render(metrics).asList(String.class);
        if (rMetrics.isEmpty()) {
            throw new IllegalArgumentException("At least one metric is required, for example UnblendedCost");
        }
        var rGroupBy = runContext.render(groupBy).asList(String.class);
        if (rGroupBy.size() > MAX_GROUP_BY) {
            throw new IllegalArgumentException("Cost Explorer supports at most " + MAX_GROUP_BY + " groupBy entries, got " + rGroupBy.size());
        }
        var rFilter = runContext.render(dimensionFilter).asMap(String.class, List.class);
        int rMaxPages = BillingFetch.maxPages(runContext, maxPages);

        var requestBuilder = GetCostAndUsageRequest.builder()
            .timePeriod(dateInterval(runContext))
            .granularity(runContext.render(granularity).as(Granularity.class).orElseThrow())
            .metrics(rMetrics)
            .groupBy(rGroupBy.stream().map(GetCostAndUsage::groupDefinition).toList());
        var filter = filter(rFilter);
        if (filter != null) {
            requestBuilder.filter(filter);
        }

        var rows = new ArrayList<Map<String, Object>>();
        var totals = new LinkedHashMap<String, BigDecimal>();
        String unit = null;
        String nextPageToken = null;
        int pages = 0;

        try (var client = client(runContext)) {
            do {
                var response = client.getCostAndUsage(requestBuilder.nextPageToken(nextPageToken).build());
                pages++;
                for (var result : response.resultsByTime()) {
                    if (rGroupBy.isEmpty()) {
                        unit = addRow(rows, totals, result, List.of(), result.total(), unit);
                    } else {
                        for (var group : result.groups()) {
                            unit = addRow(rows, totals, result, group.keys(), group.metrics(), unit);
                        }
                    }
                }
                nextPageToken = response.nextPageToken();
            } while (nextPageToken != null && pages < rMaxPages);
        }

        runContext.metric(Counter.of("requests", pages));
        if (nextPageToken != null) {
            runContext.logger().warn("Stopped after maxPages={} Cost Explorer requests, more results are available. Increase maxPages or narrow the request.", rMaxPages);
        }

        var fetched = BillingFetch.apply(runContext, runContext.render(fetchType).as(FetchType.class).orElseThrow(), rows);
        return Output.builder()
            .rows(fetched.rows())
            .row(fetched.row())
            .uri(fetched.uri())
            .size(fetched.size())
            .costTotals(totals)
            .unit(unit)
            .truncated(nextPageToken != null)
            .build();
    }

    private static String addRow(List<Map<String, Object>> rows, Map<String, BigDecimal> totals, ResultByTime result,
        List<String> keys, Map<String, MetricValue> metricValues, String unit) {
        var row = new LinkedHashMap<String, Object>();
        row.put("start", result.timePeriod().start());
        row.put("end", result.timePeriod().end());
        row.put("estimated", result.estimated());
        row.put("keys", keys);
        for (var entry : metricValues.entrySet()) {
            var amount = entry.getValue().amount() == null ? BigDecimal.ZERO : new BigDecimal(entry.getValue().amount());
            row.put(entry.getKey(), amount);
            row.put(entry.getKey() + "Unit", entry.getValue().unit());
            if (USAGE_METRICS.contains(entry.getKey())) {
                continue;
            }
            totals.merge(entry.getKey(), amount, BigDecimal::add);
            if (unit == null) {
                unit = entry.getValue().unit();
            }
        }
        rows.add(row);
        return unit;
    }

    static GroupDefinition groupDefinition(String value) {
        var key = Key.parse(value);
        return GroupDefinition.builder().type(key.type()).key(key.name()).build();
    }

    static Expression filter(Map<String, ? extends List<?>> filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }
        var expressions = new ArrayList<Expression>();
        for (var entry : filters.entrySet()) {
            var key = Key.parse(entry.getKey());
            List<String> values = entry.getValue().stream().map(String::valueOf).toList();
            expressions.add(switch (key.type()) {
                case TAG -> Expression.builder().tags(TagValues.builder().key(key.name()).values(values).build()).build();
                case COST_CATEGORY -> Expression.builder().costCategories(CostCategoryValues.builder().key(key.name()).values(values).build()).build();
                default -> Expression.builder().dimensions(DimensionValues.builder().key(key.name()).values(values).build()).build();
            });
        }
        return expressions.size() == 1 ? expressions.getFirst() : Expression.builder().and(expressions).build();
    }

    record Key(GroupDefinitionType type, String name) {
        static Key parse(String value) {
            if (value.startsWith("TAG:")) {
                return new Key(GroupDefinitionType.TAG, value.substring(4));
            }
            if (value.startsWith("COST_CATEGORY:")) {
                return new Key(GroupDefinitionType.COST_CATEGORY, value.substring(14));
            }
            return new Key(GroupDefinitionType.DIMENSION, value);
        }
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Rows",
            description = "All rows when fetchType is FETCH. Each row has `start`, `end`, `estimated`, `keys` (the group values), " +
                "and for each metric its amount (e.g. `UnblendedCost`) and unit (e.g. `UnblendedCostUnit`)."
        )
        private List<Map<String, Object>> rows;

        @Schema(title = "First row", description = "Single row when fetchType is FETCH_ONE.")
        private Map<String, Object> row;

        @Schema(title = "Stored data URI", description = "Internal storage URI of an ION file with all rows when fetchType is STORE.")
        private URI uri;

        @Schema(title = "Row count", description = "Number of rows returned by Cost Explorer.")
        private Long size;

        @Schema(
            title = "Cost totals",
            description = "Sum of each requested cost metric over all returned rows, keyed by metric name (e.g. `costTotals.UnblendedCost`). " +
                "Usage metrics (`UsageQuantity`, `NormalizedUsageAmount`) are not totaled because their units differ between rows."
        )
        private Map<String, BigDecimal> costTotals;

        @Schema(title = "Cost unit", description = "Currency of the cost metrics in `costTotals`, usually `USD`. Usage metric units are not considered.")
        private String unit;

        @Schema(title = "Truncated", description = "True when `maxPages` was reached before all results were read.")
        private Boolean truncated;
    }
}

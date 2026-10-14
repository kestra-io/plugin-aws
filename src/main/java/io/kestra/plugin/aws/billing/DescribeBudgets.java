package io.kestra.plugin.aws.billing;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.budgets.model.DescribeBudgetsRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List AWS Budgets of an account",
    description = "Calls the AWS Budgets `DescribeBudgets` API and returns each budget with its limit, actual and forecasted spend."
)
@Plugin(
    examples = {
        @Example(
            title = "List the budgets of an account and log how many there are.",
            full = true,
            code = """
                id: aws_list_budgets
                namespace: company.finops

                tasks:
                  - id: budgets
                    type: io.kestra.plugin.aws.billing.DescribeBudgets
                    accountId: "{{ secret('AWS_ACCOUNT_ID') }}"
                    fetchType: FETCH

                  - id: log_budgets
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.budgets.size }} budgets found"

                pluginDefaults:
                  - type: io.kestra.plugin.aws
                    values:
                      region: us-east-1
                      accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                      secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                """
        )
    }
)
public class DescribeBudgets extends AbstractBudgets implements RunnableTask<DescribeBudgets.Output> {
    private static final int PAGE_SIZE = 100;

    @Schema(
        title = "Maximum number of pages",
        description = "Stops after this many `DescribeBudgets` requests of 100 budgets each, and logs a warning if more budgets were available."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> maxPages = Property.ofValue(10);

    @Schema(
        title = "Fetch strategy",
        description = "Default STORE writes all budgets to internal storage. FETCH returns all budgets, FETCH_ONE the first one, NONE only the count."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rAccountId = runContext.render(accountId).as(String.class).orElseThrow();
        int rMaxPages = AbstractCostExplorer.maxPages(runContext, maxPages);

        var rows = new ArrayList<Map<String, Object>>();
        String nextToken = null;
        int pages = 0;

        try (var client = client(runContext)) {
            do {
                var response = client.describeBudgets(DescribeBudgetsRequest.builder()
                    .accountId(rAccountId)
                    .maxResults(PAGE_SIZE)
                    .nextToken(nextToken)
                    .build());
                pages++;
                response.budgets().forEach(budget -> rows.add(toMap(budget)));
                nextToken = response.nextToken();
            } while (nextToken != null && pages < rMaxPages);
        }

        if (nextToken != null) {
            runContext.logger().warn("Stopped after maxPages={} requests, more budgets are available. Increase maxPages to read them.", rMaxPages);
        }

        var fetched = BillingFetch.apply(runContext, runContext.render(fetchType).as(FetchType.class).orElseThrow(), rows);
        return Output.builder()
            .rows(fetched.rows())
            .row(fetched.row())
            .uri(fetched.uri())
            .size(fetched.size())
            .truncated(nextToken != null)
            .build();
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Budgets",
            description = "All budgets when fetchType is FETCH, each with `budgetName`, `budgetType`, `timeUnit`, `limitAmount`/`limitUnit`, " +
                "`actualSpendAmount`/`actualSpendUnit`, `forecastedSpendAmount`/`forecastedSpendUnit`, `timePeriodStart`/`timePeriodEnd` and `lastUpdatedTime`."
        )
        private List<Map<String, Object>> rows;

        @Schema(title = "First budget", description = "Single budget when fetchType is FETCH_ONE.")
        private Map<String, Object> row;

        @Schema(title = "Stored data URI", description = "Internal storage URI of an ION file with all budgets when fetchType is STORE.")
        private URI uri;

        @Schema(title = "Budget count", description = "Number of budgets returned.")
        private Long size;

        @Schema(title = "Truncated", description = "True when `maxPages` was reached before all budgets were read.")
        private Boolean truncated;
    }
}

package io.kestra.plugin.aws.billing;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.budgets.model.DescribeBudgetRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get one AWS Budget",
    description = "Calls the AWS Budgets `DescribeBudget` API and returns the budget with its limit, actual and forecasted spend."
)
@Plugin(
    examples = {
        @Example(
            title = "Fail the flow when a budget's actual spend is above its limit.",
            full = true,
            code = """
                id: aws_check_budget
                namespace: company.finops

                tasks:
                  - id: budget
                    type: io.kestra.plugin.aws.billing.DescribeBudget
                    accountId: "{{ secret('AWS_ACCOUNT_ID') }}"
                    budgetName: monthly-total

                  - id: check
                    type: io.kestra.plugin.core.execution.Fail
                    condition: "{{ outputs.budget.budget.actualSpendAmount > outputs.budget.budget.limitAmount }}"
                    errorMessage: "Budget monthly-total exceeded: {{ outputs.budget.budget.actualSpendAmount }} {{ outputs.budget.budget.actualSpendUnit }}"

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
public class DescribeBudget extends AbstractBudgets implements RunnableTask<DescribeBudget.Output> {
    @Schema(title = "Budget name", description = "Name of the budget to describe.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> budgetName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rAccountId = BillingFetch.requiredString(runContext, accountId, "accountId");
        var rBudgetName = BillingFetch.requiredString(runContext, budgetName, "budgetName");
        var request = DescribeBudgetRequest.builder()
            .accountId(rAccountId)
            .budgetName(rBudgetName)
            .build();

        try (var client = client(runContext)) {
            return Output.builder().budget(toMap(client.describeBudget(request).budget())).build();
        }
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Budget",
            description = """
                The budget with `budgetName`, `budgetType`, `timeUnit`, `limitAmount`/`limitUnit`, `actualSpendAmount`/`actualSpendUnit`, \
                `forecastedSpendAmount`/`forecastedSpendUnit`, `timePeriodStart`/`timePeriodEnd` and `lastUpdatedTime`."""
        )
        private Map<String, Object> budget;
    }
}

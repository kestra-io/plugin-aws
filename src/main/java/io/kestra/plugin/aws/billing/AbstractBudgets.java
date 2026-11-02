package io.kestra.plugin.aws.billing;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.shared.AbstractConnection;
import io.kestra.plugin.aws.shared.ConnectionUtils;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.budgets.BudgetsClient;
import software.amazon.awssdk.services.budgets.model.Budget;
import software.amazon.awssdk.services.budgets.model.Spend;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractBudgets extends AbstractConnection {
    @Schema(
        title = "AWS account ID",
        description = """
            The 12-digit ID of the account that owns the budgets. Required by the AWS Budgets API."""
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> accountId;

    @VisibleForTesting
    BudgetsClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        return ConnectionUtils.configureSyncClient(awsClientConfig(runContext), BudgetsClient.builder()).build();
    }

    static Map<String, Object> toMap(Budget budget) {
        var map = new LinkedHashMap<String, Object>();
        map.put("budgetName", budget.budgetName());
        map.put("budgetType", budget.budgetTypeAsString());
        map.put("timeUnit", budget.timeUnitAsString());
        putSpend(map, "limit", budget.budgetLimit());
        if (budget.calculatedSpend() != null) {
            putSpend(map, "actualSpend", budget.calculatedSpend().actualSpend());
            putSpend(map, "forecastedSpend", budget.calculatedSpend().forecastedSpend());
        }
        if (budget.timePeriod() != null) {
            map.put("timePeriodStart", budget.timePeriod().start() == null ? null : budget.timePeriod().start().toString());
            map.put("timePeriodEnd", budget.timePeriod().end() == null ? null : budget.timePeriod().end().toString());
        }
        map.put("lastUpdatedTime", budget.lastUpdatedTime() == null ? null : budget.lastUpdatedTime().toString());
        return map;
    }

    private static void putSpend(Map<String, Object> map, String prefix, Spend spend) {
        if (spend == null) {
            return;
        }
        map.put(prefix + "Amount", spend.amount());
        map.put(prefix + "Unit", spend.unit());
    }
}

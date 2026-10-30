package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.plugin.aws.shared.AbstractConnectionInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow when an AWS Budget crosses a spend threshold",
    description = """
        Polls one budget with the AWS Budgets `DescribeBudget` API and starts a flow when its actual or forecasted spend
        reaches `thresholdPercent` of the budget limit. The trigger fires once per budget period: after it fires, it stays
        quiet until the budget's next period (next day, month, quarter or year, depending on the budget's time unit), even
        if the spend keeps growing or the forecast goes down and up again. The period it fired for is persisted in the
        namespace KV store, so restarts and new trigger instances do not fire again. Use one trigger per threshold, for
        example one at 80% `FORECASTED` and one at 100% `ACTUAL`. The Budgets API is free, and AWS refreshes budget spend
        a few times a day, so polling more often than every hour does not bring fresher data."""
)
@Plugin(
    examples = {
        @Example(
            title = "Log a warning when the monthly budget's forecasted spend reaches 80% of its limit.",
            full = true,
            code = """
                id: aws_budget_forecast_alert
                namespace: company.finops

                tasks:
                  - id: alert
                    type: io.kestra.plugin.core.log.Log
                    level: WARN
                    message: >-
                      Budget {{ trigger.budget.budgetName }} is forecasted at {{ trigger.spendPercent }}% of its limit
                      ({{ trigger.spendAmount }} of {{ trigger.budget.limitAmount }} {{ trigger.budget.limitUnit }}) for {{ trigger.period }}

                triggers:
                  - id: budget_80_percent_forecast
                    type: io.kestra.plugin.aws.billing.BudgetThresholdTrigger
                    region: us-east-1
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    accountId: "{{ secret('AWS_ACCOUNT_ID') }}"
                    budgetName: monthly-total
                    thresholdType: FORECASTED
                    thresholdPercent: 80
                    interval: PT1H
                """
        )
    }
)
public class BudgetThresholdTrigger extends AbstractTrigger
    implements PollingTriggerInterface, TriggerOutput<BudgetThresholdTrigger.Output>, AbstractConnectionInterface {
    private static final BigDecimal HUNDRED = BigDecimal.valueOf(100);

    @Schema(title = "AWS access key ID", description = "Optional static credential. If omitted, the default credentials provider chain is used.")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    protected Property<String> accessKeyId;

    @Schema(title = "AWS secret access key", description = "Pairs with `accessKeyId` for static credentials.")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    protected Property<String> secretKeyId;

    @Schema(title = "AWS session token", description = "Session token for temporary credentials.")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    protected Property<String> sessionToken;

    @Schema(title = "AWS region", description = "AWS Budgets is a global service served from `us-east-1`.")
    @PluginProperty(group = "connection")
    protected Property<String> region;

    @Schema(title = "Endpoint override", description = "Override the default AWS endpoint URL.")
    @PluginProperty(group = "advanced")
    protected Property<String> endpointOverride;

    @Schema(title = "STS role ARN", description = "IAM role ARN to assume via STS before making API calls.")
    @PluginProperty(group = "advanced")
    protected Property<String> stsRoleArn;

    @Schema(title = "STS role external ID", description = "External ID to pass when assuming the STS role.")
    @PluginProperty(group = "advanced")
    protected Property<String> stsRoleExternalId;

    @Schema(title = "STS role session name", description = "Session name to use when assuming the STS role.")
    @PluginProperty(group = "advanced")
    protected Property<String> stsRoleSessionName;

    @Schema(title = "STS endpoint override", description = "Override the default STS endpoint URL.")
    @PluginProperty(group = "advanced")
    protected Property<String> stsEndpointOverride;

    @Schema(title = "STS role session duration", description = "Duration of the assumed role session. Default PT15M.")
    @PluginProperty(group = "execution")
    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    @Schema(
        title = "Poll interval",
        description = """
            How often to read the budget. Default `PT1H`; AWS refreshes budget spend a few times a day."""
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private final Duration interval = Duration.ofHours(1);

    @Schema(
        title = "AWS account ID",
        description = """
            The 12-digit ID of the account that owns the budget."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> accountId;

    @Schema(
        title = "Budget name",
        description = """
            Name of the budget to watch."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> budgetName;

    @Schema(
        title = "Threshold (percent of the budget limit)",
        description = """
            The trigger fires when the spend selected by `thresholdType` is greater than or equal to this percentage of the
            budget limit, for example `80` for 80%. Values above 100 alert on overspend, for example `120`."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<@Positive Double> thresholdPercent;

    @Schema(
        title = "Threshold type",
        description = """
            `ACTUAL` compares the spend so far in the current period, `FORECASTED` the spend AWS forecasts for the whole period,
            like the notification types of AWS Budgets. AWS does not forecast every budget (for example daily budgets or new
            accounts); without a forecast a `FORECASTED` trigger does not fire. Default `ACTUAL`."""
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<ThresholdType> thresholdType = Property.ofValue(ThresholdType.ACTUAL);

    @Schema(
        title = "State key",
        description = """
            Namespace KV key under which the trigger remembers the budget period it last fired for. Defaults to a key derived
            from the namespace, flow and trigger IDs. Set it to keep that memory when you rename the flow or the trigger."""
    )
    @PluginProperty(group = "advanced")
    private Property<String> stateKey;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        var rAccountId = BillingFetch.requiredString(runContext, accountId, "accountId");
        var rBudgetName = BillingFetch.requiredString(runContext, budgetName, "budgetName");
        // rendering validates @Positive, so a zero or negative threshold fails here, before any AWS call
        var rThresholdPercent = runContext.render(thresholdPercent).as(Double.class)
            .orElseThrow(() -> new IllegalArgumentException("thresholdPercent is required"));
        var rThresholdType = runContext.render(thresholdType).as(ThresholdType.class).orElse(ThresholdType.ACTUAL);
        var rStateKey = runContext.render(stateKey).as(String.class)
            .orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));

        var budget = DescribeBudget.builder()
            .id(id)
            .type(DescribeBudget.class.getName())
            .accessKeyId(accessKeyId)
            .secretKeyId(secretKeyId)
            .sessionToken(sessionToken)
            .region(region)
            .endpointOverride(endpointOverride)
            .stsRoleArn(stsRoleArn)
            .stsRoleExternalId(stsRoleExternalId)
            .stsRoleSessionName(stsRoleSessionName)
            .stsEndpointOverride(stsEndpointOverride)
            .stsRoleSessionDuration(stsRoleSessionDuration)
            .accountId(Property.ofValue(rAccountId))
            .budgetName(Property.ofValue(rBudgetName))
            .build()
            .run(runContext)
            .getBudget();

        var limit = budget.get("limitAmount") instanceof BigDecimal value ? value : null;
        if (limit == null || limit.signum() <= 0) {
            logger.warn("Budget '{}' has no positive limit, so a percentage threshold cannot be evaluated", rBudgetName);
            return Optional.empty();
        }
        var spend = budget.get(rThresholdType.spendKey()) instanceof BigDecimal value ? value : null;
        if (spend == null) {
            logger.debug("Budget '{}' has no {} spend yet", rBudgetName, rThresholdType);
            return Optional.empty();
        }

        var spendPercent = spend.multiply(HUNDRED).divide(limit, 2, RoundingMode.HALF_UP);
        if (spendPercent.compareTo(BigDecimal.valueOf(rThresholdPercent)) < 0) {
            logger.debug("Budget '{}' {} spend is at {}%, below the {}% threshold", rBudgetName, rThresholdType, spendPercent, rThresholdPercent);
            return Optional.empty();
        }

        var rPeriod = period((String) budget.get("timeUnit"), (String) budget.get("timePeriodStart"), (String) budget.get("timePeriodEnd"), today());
        var state = StatefulTriggerService.readState(runContext, rStateKey, Optional.empty());
        var thresholdId = String.join(
            "/", rAccountId, rBudgetName, rThresholdType.name(), BigDecimal.valueOf(rThresholdPercent).stripTrailingZeros().toPlainString()
        );
        var change = StatefulTriggerService.computeAndUpdateState(
            state, StatefulTriggerService.Entry.candidate(thresholdId, rPeriod, Instant.now()), StatefulTriggerInterface.On.CREATE_OR_UPDATE
        );
        if (!change.fire()) {
            logger.debug("Budget '{}' already crossed {}% of {} spend in period {}, not firing again", rBudgetName, rThresholdPercent, rThresholdType, rPeriod);
            return Optional.empty();
        }

        var output = Output.builder()
            .budget(budget)
            .thresholdType(rThresholdType)
            .thresholdPercent(rThresholdPercent)
            .spendAmount(spend)
            .spendPercent(spendPercent)
            .period(rPeriod)
            .build();
        var execution = TriggerService.generateExecution(this, conditionContext, context, output);

        // the period is persisted only once the execution exists, so a failure before this point fires again on the next poll
        StatefulTriggerService.writeState(runContext, rStateKey, state, Optional.empty());
        logger.info("Budget '{}' {} spend reached {}% of its limit in period {}", rBudgetName, rThresholdType, spendPercent, rPeriod);
        return Optional.of(execution);
    }

    @VisibleForTesting
    LocalDate today() {
        return LocalDate.now(ZoneOffset.UTC);
    }

    /**
     * Identifies the budget period that contains {@code today}. AWS Budgets periods are calendar periods in UTC; a CUSTOM
     * budget has a single period, its own time period.
     */
    static String period(String timeUnit, String timePeriodStart, String timePeriodEnd, LocalDate today) {
        return switch (timeUnit == null ? "" : timeUnit) {
            case "DAILY" -> today.toString();
            case "MONTHLY" -> YearMonth.from(today).toString();
            case "QUARTERLY" -> today.getYear() + "-Q" + ((today.getMonthValue() - 1) / 3 + 1);
            case "ANNUALLY" -> String.valueOf(today.getYear());
            default -> timePeriodStart + "/" + timePeriodEnd;
        };
    }

    public enum ThresholdType {
        ACTUAL("actualSpendAmount"),
        FORECASTED("forecastedSpendAmount");

        private final String spendKey;

        ThresholdType(String spendKey) {
            this.spendKey = spendKey;
        }

        String spendKey() {
            return spendKey;
        }
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Budget",
            description = """
                The budget as returned by `DescribeBudget`, with `budgetName`, `budgetType`, `timeUnit`, `limitAmount`/`limitUnit`, \
                `actualSpendAmount`/`actualSpendUnit`, `forecastedSpendAmount`/`forecastedSpendUnit`, `timePeriodStart`/`timePeriodEnd` and `lastUpdatedTime`."""
        )
        private Map<String, Object> budget;

        @Schema(
            title = "Threshold type",
            description = """
                Whether the actual or the forecasted spend crossed the threshold."""
        )
        private ThresholdType thresholdType;

        @Schema(
            title = "Threshold (percent)",
            description = """
                The configured `thresholdPercent`."""
        )
        private Double thresholdPercent;

        @Schema(
            title = "Spend amount",
            description = """
                The actual or forecasted spend that crossed the threshold, in the budget's unit."""
        )
        private BigDecimal spendAmount;

        @Schema(
            title = "Spend percent",
            description = """
                The spend as a percentage of the budget limit, rounded to 2 decimals."""
        )
        private BigDecimal spendPercent;

        @Schema(
            title = "Budget period",
            description = """
                The budget period the trigger fired for: `yyyy-MM-dd` for daily budgets, `yyyy-MM` for monthly, `yyyy-Qn` for \
                quarterly, `yyyy` for annual budgets, and `start/end` for custom budgets."""
        )
        private String period;
    }
}

package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.plugin.aws.shared.AbstractConnectionInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow on new AWS cost anomalies",
    description = """
        Polls the Cost Explorer `GetAnomalies` API and starts one flow execution with all the anomalies it has not reported
        before, optionally only for one monitor and above `minImpactUsd`. Each poll reads the anomalies of the last `lookback`
        days, because Cost Anomaly Detection often reports an anomaly a day or more after it started, and an ongoing anomaly
        keeps growing. The watermark of the anomalies already reported (their ID and last observed date) is persisted in the
        namespace KV store only after the execution is created, so an anomaly is reported once, even after a restart. An
        anomaly below `minImpactUsd` is not remembered, so it fires later if its impact grows above the threshold.
        Cost Explorer is a paid API: each poll costs $0.01 per page, so the default interval is `PT6H`; Cost Anomaly
        Detection itself runs about three times a day."""
)
@Plugin(
    examples = {
        @Example(
            title = "Log each new cost anomaly with a total impact of at least 100 USD.",
            full = true,
            code = """
                id: aws_cost_anomaly_alert
                namespace: company.finops

                tasks:
                  - id: each_anomaly
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.anomalies }}"
                    tasks:
                      - id: log_anomaly
                        type: io.kestra.plugin.core.log.Log
                        level: WARN
                        message: >-
                          Cost anomaly {{ fromJson(taskrun.value).anomalyId }} on {{ fromJson(taskrun.value).dimensionValue }}:
                          {{ fromJson(taskrun.value).totalImpact }} USD since {{ fromJson(taskrun.value).anomalyStartDate }}

                triggers:
                  - id: on_anomaly
                    type: io.kestra.plugin.aws.billing.AnomalyTrigger
                    region: us-east-1
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    minImpactUsd: 100
                    interval: PT6H
                """
        )
    }
)
public class AnomalyTrigger extends AbstractTrigger
    implements PollingTriggerInterface, TriggerOutput<AnomalyTrigger.Output>, AbstractConnectionInterface {
    static final Duration DEFAULT_LOOKBACK = Duration.ofDays(7);

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

    @Schema(title = "AWS region", description = "Cost Explorer is a global service served from `us-east-1`.")
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
            How often to call `GetAnomalies`. Default `PT6H`: every poll is a paid Cost Explorer request ($0.01 per page), and \
            Cost Anomaly Detection runs about three times a day."""
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private final Duration interval = Duration.ofHours(6);

    @Schema(
        title = "Monitor ARN",
        description = """
            Only report anomalies detected by this anomaly monitor. All monitors when not set."""
    )
    @PluginProperty(group = "main")
    private Property<String> monitorArn;

    @Schema(
        title = "Minimum total impact (USD)",
        description = """
            Only report anomalies whose total impact is greater than or equal to this amount. The filter is sent to AWS and \
            checked again on each anomaly. An anomaly below the threshold is not remembered, so it is reported once its impact \
            grows above it. All anomalies when not set."""
    )
    @PluginProperty(group = "main")
    private Property<@PositiveOrZero Double> minImpactUsd;

    @Schema(
        title = "Lookback",
        description = """
            How far back each poll reads anomalies, in whole days. Covers anomalies that AWS reports a few days after they \
            started, and is also how far back the first poll goes. Anomalies older than this are never reported. Default `P7D`."""
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Duration> lookback = Property.ofValue(DEFAULT_LOOKBACK);

    @Schema(
        title = "Maximum number of pages",
        description = """
            Stops after this many `GetAnomalies` requests per poll, each one costing $0.01. Anomalies on the pages not read are \
            reported by a later poll. Default 10, between 1 and 1000."""
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<@Min(1) @Max(1000) Integer> maxPages = Property.ofValue(BillingFetch.DEFAULT_MAX_PAGES);

    @Schema(
        title = "State key",
        description = """
            Namespace KV key under which the trigger persists its watermark of reported anomalies. Defaults to a key derived \
            from the namespace, flow and trigger IDs. Set it to keep the watermark when you rename the flow or the trigger."""
    )
    @PluginProperty(group = "advanced")
    private Property<String> stateKey;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        var rLookback = runContext.render(lookback).as(Duration.class).orElse(DEFAULT_LOOKBACK);
        if (rLookback.toDays() < 1) {
            throw new IllegalArgumentException("lookback must be at least 1 day (P1D), got " + rLookback);
        }
        var rMinImpactUsd = runContext.render(minImpactUsd).as(Double.class).orElse(null);
        var rStateKey = runContext.render(stateKey).as(String.class)
            .orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));

        var rToday = today();
        var windowStart = rToday.minusDays(rLookback.toDays());

        var fetched = GetAnomalies.builder()
            .id(id)
            .type(GetAnomalies.class.getName())
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
            .startDate(Property.ofValue(windowStart.toString()))
            .endDate(Property.ofValue(rToday.toString()))
            .monitorArn(monitorArn)
            .minImpactUsd(rMinImpactUsd == null ? null : Property.ofValue(rMinImpactUsd))
            .maxPages(maxPages)
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        var state = StatefulTriggerService.readState(runContext, rStateKey, Optional.empty());
        var newAnomalies = new ArrayList<Map<String, Object>>();
        for (var anomaly : fetched.getRows()) {
            if (!(anomaly.get("anomalyId") instanceof String anomalyId) || !isAboveMinImpact(anomaly, rMinImpactUsd)) {
                continue;
            }
            var observedAt = lastObservedDate(anomaly, rToday).atStartOfDay(ZoneOffset.UTC).toInstant();
            var candidate = StatefulTriggerService.Entry.candidate(anomalyId, String.valueOf(anomaly.get("anomalyStartDate")), observedAt);
            if (StatefulTriggerService.computeAndUpdateState(state, candidate, StatefulTriggerInterface.On.CREATE).fire()) {
                newAnomalies.add(anomaly);
            }
        }

        // an anomaly last observed before the window can no longer be returned, so its ID is dropped to keep the state small;
        // when pages were left unread, it may still be on them, so nothing is dropped
        if (!Boolean.TRUE.equals(fetched.getTruncated())) {
            var windowStartInstant = windowStart.atStartOfDay(ZoneOffset.UTC).toInstant();
            state.values().removeIf(entry -> entry.modifiedAt() != null && entry.modifiedAt().isBefore(windowStartInstant));
        }

        if (newAnomalies.isEmpty()) {
            StatefulTriggerService.writeState(runContext, rStateKey, state, Optional.empty());
            logger.debug("No new cost anomaly between {} and {}", windowStart, rToday);
            return Optional.empty();
        }

        newAnomalies.sort(
            Comparator.comparing((Map<String, Object> anomaly) -> String.valueOf(anomaly.get("anomalyStartDate")))
                .thenComparing(anomaly -> (String) anomaly.get("anomalyId"))
        );
        var totalImpact = newAnomalies.stream()
            .map(AnomalyTrigger::totalImpact)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        var output = Output.builder()
            .anomalies(newAnomalies)
            .size(newAnomalies.size())
            .totalImpact(totalImpact)
            .build();
        var execution = TriggerService.generateExecution(this, conditionContext, context, output);

        // the watermark moves only once the execution exists, so a failure before this point reports the anomalies again
        StatefulTriggerService.writeState(runContext, rStateKey, state, Optional.empty());
        logger.info("{} new cost anomalies with {} USD of total impact", newAnomalies.size(), totalImpact);
        return Optional.of(execution);
    }

    @VisibleForTesting
    LocalDate today() {
        return LocalDate.now(ZoneOffset.UTC);
    }

    private static boolean isAboveMinImpact(Map<String, Object> anomaly, Double minImpactUsd) {
        return minImpactUsd == null || totalImpact(anomaly).compareTo(BigDecimal.valueOf(minImpactUsd)) >= 0;
    }

    private static BigDecimal totalImpact(Map<String, Object> anomaly) {
        return anomaly.get("totalImpact") instanceof Number impact ? BigDecimal.valueOf(impact.doubleValue()) : BigDecimal.ZERO;
    }

    /**
     * The last day the anomaly was observed: its end date, or today while it is still ongoing (no end date).
     */
    static LocalDate lastObservedDate(Map<String, Object> anomaly, LocalDate today) {
        var endDate = parseDay(anomaly.get("anomalyEndDate"));
        if (endDate != null) {
            return endDate;
        }
        var startDate = parseDay(anomaly.get("anomalyStartDate"));
        return startDate != null && startDate.isAfter(today) ? startDate : today;
    }

    private static LocalDate parseDay(Object value) {
        if (!(value instanceof String date) || date.length() < 10) {
            return null;
        }
        try {
            return LocalDate.parse(date.substring(0, 10));
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "New anomalies",
            description = """
                The anomalies not reported before, oldest first, each with `anomalyId`, `anomalyStartDate`, `anomalyEndDate`, \
                `dimensionValue`, `monitorArn`, impact amounts in USD (`totalImpact`, `maxImpact`, `totalActualSpend`, \
                `totalExpectedSpend`), scores and `rootCauses`, as returned by `GetAnomalies`."""
        )
        private List<Map<String, Object>> anomalies;

        @Schema(
            title = "Anomaly count",
            description = """
                Number of new anomalies."""
        )
        private Integer size;

        @Schema(
            title = "Total impact (USD)",
            description = """
                Sum of the total impact of the new anomalies."""
        )
        private BigDecimal totalImpact;
    }
}

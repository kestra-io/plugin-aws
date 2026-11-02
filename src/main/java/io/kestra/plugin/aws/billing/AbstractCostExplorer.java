package io.kestra.plugin.aws.billing;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

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
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.costexplorer.model.DateInterval;

/**
 * Base class for AWS Cost Explorer tasks. Cost Explorer is a paid API: every paginated request costs $0.01.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCostExplorer extends AbstractConnection {
    @Schema(
        title = "Start date",
        description = """
            Start of the time period, inclusive, as `yyyy-MM-dd`."""
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> startDate;

    @Schema(
        title = "End date",
        description = """
            End of the time period, as `yyyy-MM-dd`. Exclusive for `GetCostAndUsage` and `GetCostForecast`, like in the Cost Explorer API \
            (the whole month of January is `2026-01-01` to `2026-02-01`); inclusive for `GetAnomalies` (`2026-01-01` to `2026-01-31`)."""
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> endDate;

    protected DateInterval dateInterval(RunContext runContext) throws IllegalVariableEvaluationException {
        var rStartDate = requiredDate(runContext, startDate, "startDate");
        var rEndDate = requiredDate(runContext, endDate, "endDate");
        if (!rEndDate.isAfter(rStartDate)) {
            throw new IllegalArgumentException("endDate (" + rEndDate + ") must be after startDate (" + rStartDate + "); endDate is exclusive.");
        }
        return DateInterval.builder().start(rStartDate.toString()).end(rEndDate.toString()).build();
    }

    static LocalDate requiredDate(RunContext runContext, Property<String> property, String name) throws IllegalVariableEvaluationException {
        return parseDate(name, BillingFetch.requiredString(runContext, property, name));
    }

    static LocalDate parseDate(String name, String value) {
        try {
            return LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(name + " must be a date formatted as yyyy-MM-dd, got '" + value + "'", e);
        }
    }

    @VisibleForTesting
    CostExplorerClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        return ConnectionUtils.configureSyncClient(awsClientConfig(runContext), CostExplorerClient.builder()).build();
    }
}

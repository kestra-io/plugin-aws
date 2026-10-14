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
        description = "Start of the time period, inclusive, as `yyyy-MM-dd`."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> startDate;

    @Schema(
        title = "End date",
        description = "End of the time period, as `yyyy-MM-dd`. Exclusive, like in the Cost Explorer API: " +
            "to get the whole month of January, use `2026-01-01` to `2026-02-01`."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> endDate;

    protected DateInterval dateInterval(RunContext runContext) throws IllegalVariableEvaluationException {
        var start = parseDate("startDate", runContext.render(startDate).as(String.class).orElseThrow());
        var end = parseDate("endDate", runContext.render(endDate).as(String.class).orElseThrow());
        if (!end.isAfter(start)) {
            throw new IllegalArgumentException("endDate (" + end + ") must be after startDate (" + start + "); endDate is exclusive.");
        }
        return DateInterval.builder().start(start.toString()).end(end.toString()).build();
    }

    static LocalDate parseDate(String name, String value) {
        try {
            return LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(name + " must be a date formatted as yyyy-MM-dd, got '" + value + "'", e);
        }
    }

    static int maxPages(RunContext runContext, Property<Integer> maxPages) throws IllegalVariableEvaluationException {
        int value = runContext.render(maxPages).as(Integer.class).orElseThrow();
        if (value < 1) {
            throw new IllegalArgumentException("maxPages must be at least 1, got " + value);
        }
        return value;
    }

    @VisibleForTesting
    CostExplorerClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        return ConnectionUtils.configureSyncClient(awsClientConfig(runContext), CostExplorerClient.builder()).build();
    }
}

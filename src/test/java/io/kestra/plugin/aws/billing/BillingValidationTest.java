package io.kestra.plugin.aws.billing;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.costexplorer.model.Granularity;
import software.amazon.awssdk.services.costexplorer.model.Metric;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/** Goes through Kestra's ModelValidator, the same check a flow save runs, so invalid constraint placement is caught. */
@KestraTest
class BillingValidationTest {
    @Inject
    ModelValidator modelValidator;

    static Stream<Task> tasks() {
        return Stream.of(
            GetCostAndUsage.builder().id(IdUtils.create()).type(GetCostAndUsage.class.getName())
                .startDate(Property.ofValue("2026-09-01")).endDate(Property.ofValue("2026-10-01"))
                .granularity(Property.ofValue(Granularity.MONTHLY)).metrics(Property.ofValue(List.of("UnblendedCost"))).build(),
            GetAnomalies.builder().id(IdUtils.create()).type(GetAnomalies.class.getName())
                .startDate(Property.ofValue("2026-09-01")).endDate(Property.ofValue("2026-10-01")).build(),
            GetCostForecast.builder().id(IdUtils.create()).type(GetCostForecast.class.getName())
                .startDate(Property.ofValue("2026-11-01")).endDate(Property.ofValue("2026-12-01"))
                .metric(Property.ofValue(Metric.UNBLENDED_COST)).granularity(Property.ofValue(Granularity.MONTHLY))
                .predictionIntervalLevel(Property.ofValue(80)).build(),
            DescribeBudgets.builder().id(IdUtils.create()).type(DescribeBudgets.class.getName())
                .accountId(Property.ofValue("123456789012")).build()
        );
    }

    @Test
    void givenValidTasks_whenValidate_thenNoViolation() {
        tasks().forEach(task -> assertThat(task.getType(), modelValidator.isValid(task).map(Throwable::getMessage).orElse(""), is("")));
    }

    @Test
    void givenOutOfRangeValues_whenValidate_thenViolations() {
        var cost = GetCostAndUsage.builder().id(IdUtils.create()).type(GetCostAndUsage.class.getName())
            .startDate(Property.ofValue("2026-09-01")).endDate(Property.ofValue("2026-10-01"))
            .granularity(Property.ofValue(Granularity.MONTHLY)).metrics(Property.ofValue(List.of("UnblendedCost"))).maxPages(Property.ofValue(0)).build();
        var forecast = GetCostForecast.builder().id(IdUtils.create()).type(GetCostForecast.class.getName())
            .startDate(Property.ofValue("2026-11-01")).endDate(Property.ofValue("2026-12-01"))
            .metric(Property.ofValue(Metric.UNBLENDED_COST)).granularity(Property.ofValue(Granularity.MONTHLY))
            .predictionIntervalLevel(Property.ofValue(100)).build();

        assertThat(modelValidator.isValid(cost).orElseThrow().getMessage(), containsString("maxPages"));
        assertThat(modelValidator.isValid(forecast).orElseThrow().getMessage(), containsString("predictionIntervalLevel"));
    }
}

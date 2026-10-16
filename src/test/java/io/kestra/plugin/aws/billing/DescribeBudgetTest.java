package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.budgets.BudgetsClient;
import software.amazon.awssdk.services.budgets.model.DescribeBudgetRequest;
import software.amazon.awssdk.services.budgets.model.DescribeBudgetResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DescribeBudgetTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenBudget_whenRun_thenReturnsLimitAndSpend() throws Exception {
        var task = DescribeBudget.builder()
            .id("budget")
            .type(DescribeBudget.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accountId(Property.ofValue("123456789012"))
            .budgetName(Property.ofValue("monthly-total"))
            .build();

        var client = mock(BudgetsClient.class);
        when(client.describeBudget(any(DescribeBudgetRequest.class)))
            .thenReturn(DescribeBudgetResponse.builder().budget(DescribeBudgetsTest.budget("monthly-total", "1000", "1200")).build());
        var spy = spy(task);
        doReturn(client).when(spy).client(any(RunContext.class));

        var output = spy.run(runContextFactory.of());

        assertThat(output.getBudget().get("limitAmount"), is(new BigDecimal("1000")));
        assertThat(output.getBudget().get("actualSpendAmount"), is(new BigDecimal("1200")));
        assertThat(output.getBudget().get("forecastedSpendUnit"), is("USD"));

        var captor = ArgumentCaptor.forClass(DescribeBudgetRequest.class);
        verify(client).describeBudget(captor.capture());
        assertThat(captor.getValue().budgetName(), is("monthly-total"));
        assertThat(captor.getValue().accountId(), is("123456789012"));
    }

    @Test
    void givenBudgetOverLimit_whenComparedInPebble_thenAmountsAreNumeric() throws Exception {
        var budget = AbstractBudgets.toMap(DescribeBudgetsTest.budget("monthly-total", "1000", "999.5"));
        var runContext = runContextFactory.of(Map.of("budget", budget));

        assertThat(budget.get("actualSpendAmount"), instanceOf(BigDecimal.class));
        assertThat(runContext.render("{{ budget.actualSpendAmount > budget.limitAmount }}"), is("false"));
    }
}

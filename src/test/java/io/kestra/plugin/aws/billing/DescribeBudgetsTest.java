package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.budgets.BudgetsClient;
import software.amazon.awssdk.services.budgets.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DescribeBudgetsTest {
    @Inject
    RunContextFactory runContextFactory;

    static Budget budget(String name, String limit, String actual) {
        return Budget.builder()
            .budgetName(name)
            .budgetType(BudgetType.COST)
            .timeUnit(TimeUnit.MONTHLY)
            .budgetLimit(Spend.builder().amount(new BigDecimal(limit)).unit("USD").build())
            .calculatedSpend(CalculatedSpend.builder()
                .actualSpend(Spend.builder().amount(new BigDecimal(actual)).unit("USD").build())
                .forecastedSpend(Spend.builder().amount(new BigDecimal("900")).unit("USD").build())
                .build())
            .build();
    }

    @Test
    void givenTwoPages_whenFetch_thenUsesNextTokenAndReturnsBudgets() throws Exception {
        var task = DescribeBudgets.builder()
            .id("budgets")
            .type(DescribeBudgets.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accountId(Property.ofValue("123456789012"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var client = mock(BudgetsClient.class);
        when(client.describeBudgets(any(DescribeBudgetsRequest.class)))
            .thenReturn(DescribeBudgetsResponse.builder().budgets(budget("monthly-total", "1000", "512.3")).nextToken("t2").build())
            .thenReturn(DescribeBudgetsResponse.builder().budgets(budget("team-data", "200", "10")).build());
        var spy = spy(task);
        doReturn(client).when(spy).client(any(RunContext.class));

        var output = spy.run(runContextFactory.of());

        assertThat(output.getSize(), is(2L));
        assertThat(output.getTruncated(), is(false));
        assertThat(output.getRows().get(0).get("budgetName"), is("monthly-total"));
        assertThat(output.getRows().get(0).get("actualSpendAmount"), is(new BigDecimal("512.3")));
        assertThat(output.getRows().get(0).get("timeUnit"), is("MONTHLY"));

        var captor = ArgumentCaptor.forClass(DescribeBudgetsRequest.class);
        verify(client, times(2)).describeBudgets(captor.capture());
        assertThat(captor.getAllValues().get(0).accountId(), is("123456789012"));
        assertThat(captor.getAllValues().get(1).nextToken(), is("t2"));
    }

    @Test
    void givenMaxPagesReached_whenRun_thenTruncated() throws Exception {
        var task = DescribeBudgets.builder()
            .id("budgets")
            .type(DescribeBudgets.class.getName())
            .accountId(Property.ofValue("123456789012"))
            .maxPages(Property.ofValue(1))
            .build();

        var client = mock(BudgetsClient.class);
        when(client.describeBudgets(any(DescribeBudgetsRequest.class)))
            .thenReturn(DescribeBudgetsResponse.builder().budgets(budget("a", "1", "1")).nextToken("t2").build());
        var spy = spy(task);
        doReturn(client).when(spy).client(any(RunContext.class));

        var output = spy.run(runContextFactory.of());

        verify(client, times(1)).describeBudgets(any(DescribeBudgetsRequest.class));
        assertThat(output.getTruncated(), is(true));
        assertThat(output.getUri(), notNullValue());
    }
}

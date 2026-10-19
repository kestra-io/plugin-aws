package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.costexplorer.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class GetCostAndUsageTest {
    @Inject
    RunContextFactory runContextFactory;

    private GetCostAndUsage.GetCostAndUsageBuilder<?, ?> task() {
        return GetCostAndUsage.builder()
            .id(IdUtils.create())
            .type(GetCostAndUsage.class.getName())
            .region(Property.ofValue("us-east-1"))
            .startDate(Property.ofValue("2026-09-01"))
            .endDate(Property.ofValue("2026-10-01"))
            .granularity(Property.ofValue(Granularity.MONTHLY))
            .metrics(Property.ofValue(List.of("UnblendedCost")));
    }

    private static Group group(String key, String amount) {
        return Group.builder().keys(key).metrics(Map.of("UnblendedCost", MetricValue.builder().amount(amount).unit("USD").build())).build();
    }

    private static GetCostAndUsageResponse page(String nextPageToken, Group... groups) {
        return GetCostAndUsageResponse.builder()
            .resultsByTime(
                ResultByTime.builder()
                    .timePeriod(DateInterval.builder().start("2026-09-01").end("2026-10-01").build())
                    .estimated(false)
                    .groups(groups)
                    .build()
            )
            .nextPageToken(nextPageToken)
            .build();
    }

    private GetCostAndUsage withClient(GetCostAndUsage task, CostExplorerClient client) throws Exception {
        var spy = spy(task);
        doReturn(client).when(spy).client(any(RunContext.class));
        return spy;
    }

    @Test
    void givenGroupedPages_whenFetch_thenReturnsRowsAndCostTotals() throws Exception {
        var client = mock(CostExplorerClient.class);
        when(client.getCostAndUsage(any(GetCostAndUsageRequest.class)))
            .thenReturn(page("token-2", group("Amazon EC2", "12.50")))
            .thenReturn(page(null, group("Amazon S3", "0.75")));

        var task = withClient(
            task()
                .groupBy(Property.ofValue(List.of("SERVICE", "TAG:team")))
                .dimensionFilter(Property.ofValue(Map.of("REGION", List.of("eu-west-1"))))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .build(),
            client
        );

        var output = task.run(runContextFactory.of());

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows().get(0).get("keys"), is(List.of("Amazon EC2")));
        assertThat(output.getRows().get(0).get("UnblendedCost"), is(new BigDecimal("12.50")));
        assertThat(output.getCostTotals().get("UnblendedCost"), is(new BigDecimal("13.25")));
        assertThat(output.getUnit(), is("USD"));
        assertThat(output.getTruncated(), is(false));

        var captor = ArgumentCaptor.forClass(GetCostAndUsageRequest.class);
        verify(client, times(2)).getCostAndUsage(captor.capture());
        var first = captor.getAllValues().getFirst();
        assertThat(first.timePeriod().end(), is("2026-10-01"));
        assertThat(first.groupBy().get(0).type(), is(GroupDefinitionType.DIMENSION));
        assertThat(first.groupBy().get(1).type(), is(GroupDefinitionType.TAG));
        assertThat(first.groupBy().get(1).key(), is("team"));
        assertThat(first.filter().dimensions().keyAsString(), is("REGION"));
        assertThat(captor.getAllValues().get(1).nextPageToken(), is("token-2"));
    }

    @Test
    void givenNoGroupBy_whenFetchOne_thenUsesTotal() throws Exception {
        var client = mock(CostExplorerClient.class);
        when(client.getCostAndUsage(any(GetCostAndUsageRequest.class))).thenReturn(
            GetCostAndUsageResponse.builder()
                .resultsByTime(
                    ResultByTime.builder()
                        .timePeriod(DateInterval.builder().start("2026-09-01").end("2026-10-01").build())
                        .total(Map.of("UnblendedCost", MetricValue.builder().amount("42.1").unit("USD").build()))
                        .build()
                )
                .build()
        );

        var output = withClient(task().fetchType(Property.ofValue(FetchType.FETCH_ONE)).build(), client).run(runContextFactory.of());

        assertThat(output.getRow().get("UnblendedCost"), is(new BigDecimal("42.1")));
        assertThat(output.getRow().get("keys"), is(List.of()));
        assertThat(output.getRows(), nullValue());
    }

    @Test
    void givenMorePagesThanMaxPages_whenRun_thenStopsAndFlagsTruncated() throws Exception {
        var client = mock(CostExplorerClient.class);
        when(client.getCostAndUsage(any(GetCostAndUsageRequest.class))).thenReturn(page("more", group("Amazon EC2", "1")));

        var output = withClient(task().groupBy(Property.ofValue(List.of("SERVICE"))).maxPages(Property.ofValue(3)).fetchType(Property.ofValue(FetchType.NONE)).build(), client)
            .run(runContextFactory.of());

        verify(client, times(3)).getCostAndUsage(any(GetCostAndUsageRequest.class));
        assertThat(output.getTruncated(), is(true));
        assertThat(output.getSize(), is(3L));
        assertThat(output.getCostTotals().get("UnblendedCost"), is(new BigDecimal("3")));
    }

    @Test
    void givenStore_whenRun_thenWritesFileToStorage() throws Exception {
        var client = mock(CostExplorerClient.class);
        when(client.getCostAndUsage(any(GetCostAndUsageRequest.class))).thenReturn(page(null, group("Amazon EC2", "1"), group("Amazon S3", "2")));

        var output = withClient(task().groupBy(Property.ofValue(List.of("SERVICE"))).build(), client).run(runContextFactory.of());

        assertThat(output.getUri(), notNullValue());
        assertThat(output.getSize(), is(2L));
    }

    @Test
    void givenInvalidInputs_whenRun_thenFailsBeforeCallingAws() {
        var client = mock(CostExplorerClient.class);

        assertThrows(IllegalArgumentException.class, () -> withClient(task().metrics(Property.ofValue(List.of())).build(), client).run(runContextFactory.of()));
        assertThrows(IllegalArgumentException.class, () -> withClient(task().groupBy(Property.ofValue(List.of("SERVICE", "REGION", "TAG:team"))).build(), client).run(runContextFactory.of()));
        assertThrows(IllegalArgumentException.class, () -> withClient(task().endDate(Property.ofValue("2026-09-01")).build(), client).run(runContextFactory.of()));
        assertThrows(IllegalArgumentException.class, () -> withClient(task().startDate(Property.ofValue("09/01/2026")).build(), client).run(runContextFactory.of()));
        verifyNoInteractions(client);
    }

    @Test
    void givenGroupByAndPeriodWithoutGroups_whenFetch_thenAddsNoEmptyRow() throws Exception {
        var client = mock(CostExplorerClient.class);
        when(client.getCostAndUsage(any(GetCostAndUsageRequest.class))).thenReturn(
            GetCostAndUsageResponse.builder()
                .resultsByTime(
                    ResultByTime.builder()
                        .timePeriod(DateInterval.builder().start("2026-09-01").end("2026-10-01").build())
                        .total(Map.of("UnblendedCost", MetricValue.builder().amount("0").unit("USD").build()))
                        .build()
                )
                .build()
        );

        var output = withClient(task().groupBy(Property.ofValue(List.of("SERVICE"))).fetchType(Property.ofValue(FetchType.FETCH)).build(), client)
            .run(runContextFactory.of());

        assertThat(output.getSize(), is(0L));
        assertThat(output.getRows(), empty());
    }

    @Test
    void givenCostAndUsageMetrics_whenRun_thenTotalsOnlyCostMetrics() throws Exception {
        var client = mock(CostExplorerClient.class);
        when(client.getCostAndUsage(any(GetCostAndUsageRequest.class))).thenReturn(
            GetCostAndUsageResponse.builder()
                .resultsByTime(
                    ResultByTime.builder()
                        .timePeriod(DateInterval.builder().start("2026-09-01").end("2026-10-01").build())
                        .total(
                            Map.of(
                                "UsageQuantity", MetricValue.builder().amount("730").unit("Hrs").build(),
                                "UnblendedCost", MetricValue.builder().amount("5").unit("USD").build()
                            )
                        )
                        .build()
                )
                .build()
        );

        var output = withClient(
            task().metrics(Property.ofValue(List.of("UsageQuantity", "UnblendedCost"))).fetchType(Property.ofValue(FetchType.FETCH)).build(),
            client
        ).run(runContextFactory.of());

        assertThat(output.getCostTotals().keySet(), is(java.util.Set.of("UnblendedCost")));
        assertThat(output.getUnit(), is("USD"));
        assertThat(output.getRows().getFirst().get("UsageQuantity"), is(new BigDecimal("730")));
    }

    @Test
    void givenFetchOneAndNextPageToken_whenRun_thenStopsAfterFirstPageWithRows() throws Exception {
        var client = mock(CostExplorerClient.class);
        when(client.getCostAndUsage(any(GetCostAndUsageRequest.class))).thenReturn(page("more", group("Amazon EC2", "1")));

        var output = withClient(task().groupBy(Property.ofValue(List.of("SERVICE"))).fetchType(Property.ofValue(FetchType.FETCH_ONE)).build(), client)
            .run(runContextFactory.of());

        verify(client, times(1)).getCostAndUsage(any(GetCostAndUsageRequest.class));
        assertThat(output.getRow().get("keys"), is(List.of("Amazon EC2")));
        assertThat(output.getTruncated(), is(true));
    }

    @Test
    void givenBlankStartDate_whenRun_thenFailsWithClearMessageBeforeCallingAws() {
        var client = mock(CostExplorerClient.class);

        var exception = assertThrows(IllegalArgumentException.class, () -> withClient(task().startDate(Property.ofValue(" ")).build(), client).run(runContextFactory.of()));

        assertThat(exception.getMessage(), Matchers.startsWith("startDate is required"));
        verifyNoInteractions(client);
    }
}

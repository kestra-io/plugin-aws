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
class GetAnomaliesTest {
    @Inject
    RunContextFactory runContextFactory;

    private static Anomaly anomaly(String id, double impact) {
        return Anomaly.builder()
            .anomalyId(id)
            .anomalyStartDate("2026-10-10")
            .dimensionValue("Amazon EC2")
            .monitorArn("arn:aws:ce::123456789012:anomalymonitor/abc")
            .anomalyScore(AnomalyScore.builder().maxScore(0.9).currentScore(0.8).build())
            .impact(Impact.builder().maxImpact(impact).totalImpact(impact).totalActualSpend(impact + 10).totalExpectedSpend(10.0).build())
            .rootCauses(RootCause.builder().service("Amazon EC2").region("us-east-1").usageType("BoxUsage").build())
            .build();
    }

    @Test
    void givenTwoPages_whenFetch_thenReturnsAnomaliesAndTotalImpact() throws Exception {
        var task = GetAnomalies.builder()
            .id(IdUtils.create())
            .type(GetAnomalies.class.getName())
            .region(Property.ofValue("us-east-1"))
            .startDate(Property.ofValue("2026-10-07"))
            .endDate(Property.ofValue("2026-10-14"))
            .monitorArn(Property.ofValue("arn:aws:ce::123456789012:anomalymonitor/abc"))
            .minImpactUsd(Property.ofValue(100.0))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var client = mock(CostExplorerClient.class);
        when(client.getAnomalies(any(GetAnomaliesRequest.class)))
            .thenReturn(GetAnomaliesResponse.builder().anomalies(anomaly("a-1", 150.5)).nextPageToken("next").build())
            .thenReturn(GetAnomaliesResponse.builder().anomalies(anomaly("a-2", 200.0)).build());
        var spy = spy(task);
        doReturn(client).when(spy).client(any(RunContext.class));

        var output = spy.run(runContextFactory.of());

        assertThat(output.getSize(), is(2L));
        assertThat(output.getTotalImpact(), comparesEqualTo(new BigDecimal("350.5")));
        assertThat(output.getRows().get(0).get("anomalyId"), is("a-1"));
        @SuppressWarnings("unchecked")
        var rootCauses = (List<Map<String, Object>>) output.getRows().get(0).get("rootCauses");
        assertThat(rootCauses.getFirst().get("service"), is("Amazon EC2"));

        var captor = ArgumentCaptor.forClass(GetAnomaliesRequest.class);
        verify(client, times(2)).getAnomalies(captor.capture());
        var first = captor.getAllValues().getFirst();
        assertThat(first.dateInterval().startDate(), is("2026-10-07"));
        assertThat(first.totalImpact().numericOperator(), is(NumericOperator.GREATER_THAN_OR_EQUAL));
        assertThat(first.totalImpact().startValue(), is(100.0));
        assertThat(captor.getAllValues().get(1).nextPageToken(), is("next"));
    }

    @Test
    void givenSameStartAndEndDate_whenRun_thenSendsInclusiveInterval() throws Exception {
        var task = GetAnomalies.builder()
            .id(IdUtils.create())
            .type(GetAnomalies.class.getName())
            .region(Property.ofValue("us-east-1"))
            .startDate(Property.ofValue("2026-10-14"))
            .endDate(Property.ofValue("2026-10-14"))
            .fetchType(Property.ofValue(FetchType.NONE))
            .build();

        var client = mock(CostExplorerClient.class);
        when(client.getAnomalies(any(GetAnomaliesRequest.class))).thenReturn(GetAnomaliesResponse.builder().build());
        var spy = spy(task);
        doReturn(client).when(spy).client(any(RunContext.class));

        spy.run(runContextFactory.of());

        var captor = ArgumentCaptor.forClass(GetAnomaliesRequest.class);
        verify(client).getAnomalies(captor.capture());
        assertThat(captor.getValue().dateInterval().startDate(), is("2026-10-14"));
        assertThat(captor.getValue().dateInterval().endDate(), is("2026-10-14"));
    }

    @Test
    void givenFetchOneAndNextPageToken_whenRun_thenStopsAfterFirstPageWithRows() throws Exception {
        var task = GetAnomalies.builder()
            .id(IdUtils.create())
            .type(GetAnomalies.class.getName())
            .region(Property.ofValue("us-east-1"))
            .startDate(Property.ofValue("2026-10-07"))
            .endDate(Property.ofValue("2026-10-14"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        var client = mock(CostExplorerClient.class);
        when(client.getAnomalies(any(GetAnomaliesRequest.class)))
            .thenReturn(GetAnomaliesResponse.builder().anomalies(anomaly("a-1", 150.5)).nextPageToken("next").build());
        var spy = spy(task);
        doReturn(client).when(spy).client(any(RunContext.class));

        var output = spy.run(runContextFactory.of());

        verify(client, times(1)).getAnomalies(any(GetAnomaliesRequest.class));
        assertThat(output.getRow().get("anomalyId"), is("a-1"));
        assertThat(output.getTruncated(), is(false));
    }

    @Test
    void givenEmptyEndDate_whenRun_thenFailsWithClearMessageBeforeCallingAws() throws Exception {
        var task = GetAnomalies.builder()
            .id(IdUtils.create())
            .type(GetAnomalies.class.getName())
            .region(Property.ofValue("us-east-1"))
            .startDate(Property.ofValue("2026-10-07"))
            .endDate(Property.ofValue(""))
            .build();

        var client = mock(CostExplorerClient.class);
        var spy = spy(task);
        doReturn(client).when(spy).client(any(RunContext.class));

        var exception = assertThrows(IllegalArgumentException.class, () -> spy.run(runContextFactory.of()));

        assertThat(exception.getMessage(), Matchers.startsWith("endDate is required"));
        verifyNoInteractions(client);
    }
}

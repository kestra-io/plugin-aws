package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/** Runs the trigger against WireMock with the real Cost Explorer client, and the real namespace KV store for its watermark. */
@KestraTest
class AnomalyTriggerTest {
    private static final String GET_ANOMALIES = "AWSInsightsIndexService.GetAnomalies";
    private static final LocalDate TODAY = LocalDate.of(2026, 10, 20);

    @Inject
    RunContextFactory runContextFactory;

    private WireMockServer server;

    @BeforeEach
    void start() {
        server = new WireMockServer(options().dynamicPort());
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    @Test
    void givenNoAnomalies_whenEvaluate_thenNoExecutionAndQueriesLookbackWindow() throws Exception {
        stubAnomalies();
        var trigger = spyToday(rawTrigger(null), TODAY);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
        server.verify(
            postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo(GET_ANOMALIES))
                .withRequestBody(matchingJsonPath("$.DateInterval.StartDate", equalTo("2026-10-13")))
                .withRequestBody(matchingJsonPath("$.DateInterval.EndDate", equalTo("2026-10-20")))
                .withRequestBody(matchingJsonPath("$.MonitorArn", equalTo("arn:aws:ce::123456789012:anomalymonitor/m")))
        );
    }

    @Test
    void givenReportedAnomalies_whenEvaluateAgain_thenOnlyAnomaliesNewerThanWatermarkFire() throws Exception {
        stubAnomalies(anomaly("a-1", "2026-10-17", null, 150.0), anomaly("a-2", "2026-10-18", "2026-10-19", 220.0));
        var raw = rawTrigger(null);
        var trigger = spyToday(raw, TODAY);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(anomalyIds(first), contains("a-1", "a-2"));
        assertThat(new BigDecimal(first.orElseThrow().getTrigger().getVariables().get("totalImpact").toString()), comparesEqualTo(new BigDecimal("370")));
        assertThat(first.orElseThrow().getTrigger().getVariables().get("size"), is(2));
        assertThat(persistedIds(context, raw), containsInAnyOrder("a-1", "a-2"));

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        // a new instance reads the persisted watermark; a-1 is still ongoing and grew, but it was already reported
        stubAnomalies(anomaly("a-1", "2026-10-17", null, 400.0), anomaly("a-2", "2026-10-18", "2026-10-19", 220.0), anomaly("a-3", "2026-10-19", null, 90.0));
        var restarted = spyToday(raw, TODAY.plusDays(1));
        var second = restarted.evaluate(context.getKey(), context.getValue());
        assertThat(anomalyIds(second), contains("a-3"));
        assertThat(persistedIds(context, raw), containsInAnyOrder("a-1", "a-2", "a-3"));
    }

    @Test
    void givenAnomalyDetectedLateWithOlderStartDate_whenEvaluate_thenFires() throws Exception {
        stubAnomalies(anomaly("a-1", "2026-10-19", null, 150.0));
        var trigger = spyToday(rawTrigger(null), TODAY);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        assertThat(anomalyIds(trigger.evaluate(context.getKey(), context.getValue())), contains("a-1"));

        stubAnomalies(anomaly("a-1", "2026-10-19", null, 150.0), anomaly("a-late", "2026-10-16", null, 300.0));
        assertThat(anomalyIds(trigger.evaluate(context.getKey(), context.getValue())), contains("a-late"));
    }

    @Test
    void givenMinImpactUsd_whenEvaluate_thenFiltersAndReportsAnomalyOnceItGrowsAboveThreshold() throws Exception {
        stubAnomalies(anomaly("a-small", "2026-10-18", null, 50.0), anomaly("a-big", "2026-10-18", null, 150.0));
        var raw = rawTrigger(100.0);
        var trigger = spyToday(raw, TODAY);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(anomalyIds(trigger.evaluate(context.getKey(), context.getValue())), contains("a-big"));
        assertThat(persistedIds(context, raw), contains("a-big"));
        server.verify(
            postRequestedFor(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$.TotalImpact.NumericOperator", equalTo("GREATER_THAN_OR_EQUAL")))
                .withRequestBody(matchingJsonPath("$.TotalImpact.StartValue", equalTo("100.0")))
        );

        stubAnomalies(anomaly("a-small", "2026-10-18", null, 120.0), anomaly("a-big", "2026-10-18", null, 160.0));
        assertThat(anomalyIds(trigger.evaluate(context.getKey(), context.getValue())), contains("a-small"));
    }

    @Test
    void givenAnomalyOutsideLookback_whenEvaluate_thenDropsItFromWatermark() throws Exception {
        stubAnomalies(anomaly("a-1", "2026-10-15", "2026-10-16", 150.0));
        var raw = rawTrigger(null);
        var trigger = spyToday(raw, TODAY);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        assertThat(anomalyIds(trigger.evaluate(context.getKey(), context.getValue())), contains("a-1"));

        stubAnomalies(anomaly("a-1", "2026-10-15", "2026-10-16", 150.0));
        var stillInWindow = spyToday(raw, LocalDate.of(2026, 10, 23));
        assertThat(stillInWindow.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
        assertThat(persistedIds(context, raw), contains("a-1"));

        stubAnomalies();
        var tenDaysLater = spyToday(raw, LocalDate.of(2026, 10, 30));
        assertThat(tenDaysLater.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
        assertThat(persistedIds(context, raw), empty());
    }

    private AnomalyTrigger rawTrigger(Double minImpactUsd) {
        return AnomalyTrigger.builder()
            .id(IdUtils.create())
            .type(AnomalyTrigger.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test"))
            .secretKeyId(Property.ofValue("test"))
            .endpointOverride(Property.ofValue(server.baseUrl()))
            .monitorArn(Property.ofValue("arn:aws:ce::123456789012:anomalymonitor/m"))
            .minImpactUsd(minImpactUsd == null ? null : Property.ofValue(minImpactUsd))
            .build();
    }

    private static AnomalyTrigger spyToday(AnomalyTrigger trigger, LocalDate today) {
        var spy = spy(trigger);
        doReturn(today.atTime(12, 0).toInstant(ZoneOffset.UTC)).when(spy).now();
        return spy;
    }

    @SuppressWarnings("unchecked")
    private static List<String> anomalyIds(Optional<Execution> execution) {
        assertThat(execution.isPresent(), is(true));
        var anomalies = (List<Map<String, Object>>) execution.orElseThrow().getTrigger().getVariables().get("anomalies");
        return anomalies.stream().map(anomaly -> (String) anomaly.get("anomalyId")).toList();
    }

    private static List<String> persistedIds(Map.Entry<ConditionContext, Trigger> context, AnomalyTrigger trigger) {
        var key = StatefulTriggerService.defaultKey(context.getValue().getNamespace(), context.getValue().getFlowId(), trigger.getId());
        return StatefulTriggerService.readState(context.getKey().getRunContext(), key, Optional.empty()).keySet().stream()
            .sorted()
            .toList();
    }

    private void stubAnomalies(String... anomalies) {
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(GET_ANOMALIES))
                .willReturn(okJson("{\"Anomalies\":[" + String.join(",", anomalies) + "]}"))
        );
    }

    private static String anomaly(String id, String startDate, String endDate, double impact) {
        return """
            {"AnomalyId":"%s","AnomalyStartDate":"%s",%s"DimensionValue":"Amazon EC2","MonitorArn":"arn:aws:ce::123456789012:anomalymonitor/m",
              "AnomalyScore":{"MaxScore":0.9,"CurrentScore":0.8},"Impact":{"MaxImpact":%s,"TotalImpact":%s}}
            """.formatted(id, startDate, endDate == null ? "" : "\"AnomalyEndDate\":\"" + endDate + "\",", impact, impact);
    }
}

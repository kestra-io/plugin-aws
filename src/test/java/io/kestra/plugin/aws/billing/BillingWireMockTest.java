package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.costexplorer.model.Granularity;
import software.amazon.awssdk.services.costexplorer.model.Metric;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Runs the real AWS SDK clients against WireMock, so request marshalling and client configuration are exercised. */
@KestraTest
class BillingWireMockTest {
    private static final String CE_TARGET = "AWSInsightsIndexService.GetCostAndUsage";
    private static final String BUDGETS_TARGET = "AWSBudgetServiceGateway.DescribeBudget";
    private static final String BUDGETS_LIST_TARGET = "AWSBudgetServiceGateway.DescribeBudgets";
    private static final String ANOMALIES_TARGET = "AWSInsightsIndexService.GetAnomalies";
    private static final String FORECAST_TARGET = "AWSInsightsIndexService.GetCostForecast";

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
    void givenTwoPages_whenGetCostAndUsage_thenSendsFilterGroupByAndNextPageToken() throws Exception {
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(CE_TARGET)).withRequestBody(notContaining("NextPageToken"))
                .willReturn(okJson(page("Amazon EC2", "12.5", "token-2")))
        );
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(CE_TARGET)).withRequestBody(matchingJsonPath("$.NextPageToken", equalTo("token-2")))
                .willReturn(okJson(page("Amazon S3", "0.75", null)))
        );

        var task = GetCostAndUsage.builder()
            .id(IdUtils.create())
            .type(GetCostAndUsage.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test"))
            .secretKeyId(Property.ofValue("test"))
            .endpointOverride(Property.ofValue(server.baseUrl()))
            .startDate(Property.ofValue("2026-09-01"))
            .endDate(Property.ofValue("2026-10-01"))
            .granularity(Property.ofValue(Granularity.MONTHLY))
            .metrics(Property.ofValue(List.of("UnblendedCost")))
            .groupBy(Property.ofValue(List.of("SERVICE")))
            .dimensionFilter(Property.ofValue(Map.of("REGION", List.of("eu-west-1"))))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getSize(), is(2L));
        assertThat(output.getCostTotals().get("UnblendedCost").toPlainString(), is("13.25"));
        server.verify(
            postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo(CE_TARGET))
                .withHeader("Authorization", containing("AWS4-HMAC-SHA256"))
                .withRequestBody(matchingJsonPath("$.TimePeriod.Start", equalTo("2026-09-01")))
                .withRequestBody(matchingJsonPath("$.Granularity", equalTo("MONTHLY")))
                .withRequestBody(matchingJsonPath("$.GroupBy[0].Type", equalTo("DIMENSION")))
                .withRequestBody(matchingJsonPath("$.GroupBy[0].Key", equalTo("SERVICE")))
                .withRequestBody(matchingJsonPath("$.Filter.Dimensions.Key", equalTo("REGION")))
                .withRequestBody(matchingJsonPath("$.Filter.Dimensions.Values[0]", equalTo("eu-west-1")))
        );
        server.verify(2, postRequestedFor(urlEqualTo("/")).withHeader("X-Amz-Target", equalTo(CE_TARGET)));
    }

    @Test
    void givenUnknownBudget_whenDescribeBudget_thenFailsWithClearMessage() {
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(BUDGETS_TARGET))
                .willReturn(
                    aResponse().withStatus(400).withHeader("Content-Type", "application/x-amz-json-1.1")
                        .withBody("{\"__type\":\"NotFoundException\",\"Message\":\"Unable to get budget: typo\"}")
                )
        );

        var task = DescribeBudget.builder()
            .id(IdUtils.create())
            .type(DescribeBudget.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test"))
            .secretKeyId(Property.ofValue("test"))
            .endpointOverride(Property.ofValue(server.baseUrl()))
            .accountId(Property.ofValue("123456789012"))
            .budgetName(Property.ofValue("typo"))
            .build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("Budget 'typo' not found in account 123456789012"));
        server.verify(
            postRequestedFor(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$.AccountId", equalTo("123456789012")))
                .withRequestBody(matchingJsonPath("$.BudgetName", equalTo("typo")))
        );
    }

    @Test
    void givenTwoPages_whenDescribeBudgets_thenSendsNextToken() throws Exception {
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(BUDGETS_LIST_TARGET)).withRequestBody(notContaining("NextToken"))
                .willReturn(okJson("{\"Budgets\":[{\"BudgetName\":\"team-a\",\"BudgetType\":\"COST\",\"TimeUnit\":\"MONTHLY\"}],\"NextToken\":\"t2\"}"))
        );
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(BUDGETS_LIST_TARGET)).withRequestBody(matchingJsonPath("$.NextToken", equalTo("t2")))
                .willReturn(okJson("{\"Budgets\":[{\"BudgetName\":\"team-b\",\"BudgetType\":\"COST\",\"TimeUnit\":\"MONTHLY\"}]}"))
        );

        var task = DescribeBudgets.builder()
            .id(IdUtils.create())
            .type(DescribeBudgets.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test"))
            .secretKeyId(Property.ofValue("test"))
            .endpointOverride(Property.ofValue(server.baseUrl()))
            .accountId(Property.ofValue("123456789012"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getRows().stream().map(row -> row.get("budgetName")).toList(), contains("team-a", "team-b"));
        assertThat(output.getTruncated(), is(false));
        server.verify(2, postRequestedFor(urlEqualTo("/")).withRequestBody(matchingJsonPath("$.AccountId", equalTo("123456789012"))));
    }

    @Test
    void givenTwoPages_whenGetAnomalies_thenSendsFilterAndNextPageToken() throws Exception {
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(ANOMALIES_TARGET)).withRequestBody(notContaining("NextPageToken"))
                .willReturn(okJson(anomaly("a-1", 100.0, "p2")))
        );
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(ANOMALIES_TARGET)).withRequestBody(matchingJsonPath("$.NextPageToken", equalTo("p2")))
                .willReturn(okJson(anomaly("a-2", 50.5, null)))
        );

        var task = GetAnomalies.builder()
            .id(IdUtils.create())
            .type(GetAnomalies.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test"))
            .secretKeyId(Property.ofValue("test"))
            .endpointOverride(Property.ofValue(server.baseUrl()))
            .startDate(Property.ofValue("2026-09-01"))
            .endDate(Property.ofValue("2026-10-01"))
            .minImpactUsd(Property.ofValue(10.0))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getRows().stream().map(row -> row.get("anomalyId")).toList(), contains("a-1", "a-2"));
        assertThat(output.getTotalImpact(), comparesEqualTo(new BigDecimal("150.5")));
        server.verify(
            postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo(ANOMALIES_TARGET))
                .withRequestBody(matchingJsonPath("$.DateInterval.StartDate", equalTo("2026-09-01")))
                .withRequestBody(matchingJsonPath("$.TotalImpact.NumericOperator", equalTo("GREATER_THAN_OR_EQUAL")))
        );
        server.verify(2, postRequestedFor(urlEqualTo("/")).withHeader("X-Amz-Target", equalTo(ANOMALIES_TARGET)));
    }

    @Test
    void givenForecast_whenGetCostForecast_thenSendsRequestAndMapsTotals() throws Exception {
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(FORECAST_TARGET))
                .willReturn(okJson("""
                    {"Total":{"Amount":"420.5","Unit":"USD"},"ForecastResultsByTime":[{"TimePeriod":{"Start":"2026-11-01","End":"2026-12-01"},
                      "MeanValue":"420.5","PredictionIntervalLowerBound":"400","PredictionIntervalUpperBound":"441"}]}
                    """))
        );

        var task = GetCostForecast.builder()
            .id(IdUtils.create())
            .type(GetCostForecast.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test"))
            .secretKeyId(Property.ofValue("test"))
            .endpointOverride(Property.ofValue(server.baseUrl()))
            .startDate(Property.ofValue("2026-11-01"))
            .endDate(Property.ofValue("2026-12-01"))
            .metric(Property.ofValue(Metric.UNBLENDED_COST))
            .granularity(Property.ofValue(Granularity.MONTHLY))
            .predictionIntervalLevel(Property.ofValue(80))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getTotalAmount(), comparesEqualTo(new BigDecimal("420.5")));
        assertThat(output.getUnit(), is("USD"));
        assertThat(output.getForecast().getFirst().get("upperBound"), is(new BigDecimal("441")));
        server.verify(
            postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo(FORECAST_TARGET))
                .withRequestBody(matchingJsonPath("$.Metric", equalTo("UNBLENDED_COST")))
                .withRequestBody(matchingJsonPath("$.PredictionIntervalLevel", equalTo("80")))
        );
    }

    private static String anomaly(String id, double impact, String nextPageToken) {
        return """
            {"Anomalies":[{"AnomalyId":"%s","AnomalyStartDate":"2026-09-02","DimensionValue":"Amazon EC2","MonitorArn":"arn:aws:ce::1:anomalymonitor/m",
              "AnomalyScore":{"MaxScore":0.9,"CurrentScore":0.8},"Impact":{"MaxImpact":%s,"TotalImpact":%s}}]%s}
            """.formatted(id, impact, impact, nextPageToken == null ? "" : ",\"NextPageToken\":\"" + nextPageToken + "\"");
    }

    private static String page(String service, String amount, String nextPageToken) {
        return """
            {"ResultsByTime":[{"TimePeriod":{"Start":"2026-09-01","End":"2026-10-01"},"Estimated":false,
              "Groups":[{"Keys":["%s"],"Metrics":{"UnblendedCost":{"Amount":"%s","Unit":"USD"}}}]}]%s}
            """.formatted(service, amount, nextPageToken == null ? "" : ",\"NextPageToken\":\"" + nextPageToken + "\"");
    }
}

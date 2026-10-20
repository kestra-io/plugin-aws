package io.kestra.plugin.aws.billing;

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

    private static String page(String service, String amount, String nextPageToken) {
        return """
            {"ResultsByTime":[{"TimePeriod":{"Start":"2026-09-01","End":"2026-10-01"},"Estimated":false,
              "Groups":[{"Keys":["%s"],"Metrics":{"UnblendedCost":{"Amount":"%s","Unit":"USD"}}}]}]%s}
            """.formatted(service, amount, nextPageToken == null ? "" : ",\"NextPageToken\":\"" + nextPageToken + "\"");
    }
}

package io.kestra.plugin.aws.billing;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolationException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/** Runs the trigger against WireMock with the real Budgets client, and the real namespace KV store for its state. */
@KestraTest
class BudgetThresholdTriggerTest {
    private static final String DESCRIBE_BUDGET = "AWSBudgetServiceGateway.DescribeBudget";
    private static final LocalDate OCTOBER = LocalDate.of(2026, 10, 20);

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
    void givenActualSpendBelowThreshold_whenEvaluate_thenNoExecution() throws Exception {
        stubBudget("100", "79.99", "130");
        var trigger = trigger(BudgetThresholdTrigger.ThresholdType.ACTUAL, 80.0, OCTOBER);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
        server.verify(
            postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo(DESCRIBE_BUDGET))
                .withRequestBody(matchingJsonPath("$.AccountId", equalTo("123456789012")))
                .withRequestBody(matchingJsonPath("$.BudgetName", equalTo("monthly-total")))
        );
    }

    @Test
    void givenActualSpendAtThreshold_whenEvaluate_thenFiresWithSpendPercentAndPeriod() throws Exception {
        stubBudget("100", "80", "95");
        var trigger = trigger(BudgetThresholdTrigger.ThresholdType.ACTUAL, 80.0, OCTOBER);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        var variables = execution.orElseThrow().getTrigger().getVariables();
        assertThat(new BigDecimal(variables.get("spendPercent").toString()), comparesEqualTo(new BigDecimal("80")));
        assertThat(new BigDecimal(variables.get("spendAmount").toString()), comparesEqualTo(new BigDecimal("80")));
        assertThat(variables.get("thresholdType"), is("ACTUAL"));
        assertThat(variables.get("period"), is("2026-10"));
    }

    @Test
    void givenForecastAboveAndActualBelow_whenEvaluate_thenOnlyForecastedTriggerFires() throws Exception {
        stubBudget("100", "60", "112.5");

        var forecasted = trigger(BudgetThresholdTrigger.ThresholdType.FORECASTED, 100.0, OCTOBER);
        var forecastedContext = TestsUtils.mockTrigger(runContextFactory, forecasted);
        var execution = forecasted.evaluate(forecastedContext.getKey(), forecastedContext.getValue());
        assertThat(execution.isPresent(), is(true));
        assertThat(new BigDecimal(execution.orElseThrow().getTrigger().getVariables().get("spendPercent").toString()), comparesEqualTo(new BigDecimal("112.5")));
        assertThat(execution.orElseThrow().getTrigger().getVariables().get("thresholdType"), is("FORECASTED"));

        var actual = trigger(BudgetThresholdTrigger.ThresholdType.ACTUAL, 100.0, OCTOBER);
        var actualContext = TestsUtils.mockTrigger(runContextFactory, actual);
        assertThat(actual.evaluate(actualContext.getKey(), actualContext.getValue()).isPresent(), is(false));
    }

    @Test
    void givenNoForecastYet_whenEvaluateForecasted_thenNoExecution() throws Exception {
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(DESCRIBE_BUDGET))
                .willReturn(okJson("""
                    {"Budget":{"BudgetName":"monthly-total","BudgetType":"COST","TimeUnit":"MONTHLY",
                      "BudgetLimit":{"Amount":"100","Unit":"USD"},"CalculatedSpend":{"ActualSpend":{"Amount":"150","Unit":"USD"}}}}
                    """))
        );
        var trigger = trigger(BudgetThresholdTrigger.ThresholdType.FORECASTED, 80.0, OCTOBER);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
    }

    @Test
    void givenAlreadyFiredThisPeriod_whenEvaluateAgain_thenDoesNotFireUntilNextPeriod() throws Exception {
        stubBudget("100", "90", "120");
        var raw = rawTrigger(BudgetThresholdTrigger.ThresholdType.ACTUAL, 80.0);
        var october = spyToday(raw, OCTOBER);
        var context = TestsUtils.mockTrigger(runContextFactory, october);

        assertThat(october.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));
        assertThat(october.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        // a new trigger instance reads the persisted state, so a restart does not fire again in the same period
        var sameMonth = spyToday(raw, OCTOBER.plusDays(5));
        assertThat(sameMonth.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        var november = spyToday(raw, LocalDate.of(2026, 11, 2));
        var execution = november.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));
        assertThat(execution.orElseThrow().getTrigger().getVariables().get("period"), is("2026-11"));
        assertThat(november.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
    }

    @Test
    void givenStaleSpendRightAfterPeriodRollover_whenEvaluate_thenWaitsForPeriodStartGrace() throws Exception {
        stubBudget("100", "90", "120");
        var raw = rawTrigger(BudgetThresholdTrigger.ThresholdType.ACTUAL, 80.0);
        var october = spyToday(raw, OCTOBER);
        var context = TestsUtils.mockTrigger(runContextFactory, october);
        assertThat(october.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));

        // two hours into November, AWS still returns October's spend: firing now would mark November as alerted
        var earlyNovember = spyAt(raw, Instant.parse("2026-11-01T02:00:00Z"));
        assertThat(earlyNovember.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        var justBeforeGraceEnd = spyAt(raw, Instant.parse("2026-11-01T11:59:59Z"));
        assertThat(justBeforeGraceEnd.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        // the default 12h grace ends exactly at start + 12h
        var atGraceEnd = spyAt(raw, Instant.parse("2026-11-01T12:00:00Z"));
        var execution = atGraceEnd.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));
        assertThat(execution.orElseThrow().getTrigger().getVariables().get("period"), is("2026-11"));
    }

    @Test
    void givenDailyBudget_whenEvaluate_thenGraceIsCappedAtAQuarterOfTheDay() throws Exception {
        stubBudget("DAILY", "100", "90", "120");
        var raw = rawTrigger(BudgetThresholdTrigger.ThresholdType.ACTUAL, 80.0);
        var blocked = spyAt(raw, Instant.parse("2026-10-21T05:59:00Z"));
        var context = TestsUtils.mockTrigger(runContextFactory, blocked);

        // the default 12h grace would block a daily budget until noon; capped, it ends at 06:00
        assertThat(blocked.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        var execution = spyAt(raw, Instant.parse("2026-10-21T06:00:00Z")).evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));
        assertThat(execution.orElseThrow().getTrigger().getVariables().get("period"), is("2026-10-21"));
    }

    @Test
    void givenNegativePeriodStartGrace_whenEvaluate_thenFailsBeforeCallingAws() {
        var raw = rawTrigger(BudgetThresholdTrigger.ThresholdType.ACTUAL, 80.0, Duration.ofHours(-1));
        var trigger = spyToday(raw, OCTOBER);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var e = assertThrows(IllegalArgumentException.class, () -> trigger.evaluate(context.getKey(), context.getValue()));
        assertThat(e.getMessage(), containsString("periodStartGrace must not be negative"));
        server.verify(0, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void givenZeroThreshold_whenEvaluate_thenFailsBeforeCallingAws() {
        var trigger = trigger(BudgetThresholdTrigger.ThresholdType.ACTUAL, 0.0, OCTOBER);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var e = assertThrows(ConstraintViolationException.class, () -> trigger.evaluate(context.getKey(), context.getValue()));
        assertThat(e.getMessage(), containsString("thresholdPercent: must be greater than 0"));
        server.verify(0, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void givenTimeUnit_whenPeriod_thenReturnsCalendarPeriodInUtc() {
        var day = LocalDate.of(2026, 8, 15);
        assertThat(BudgetThresholdTrigger.period("DAILY", null, null, day), is("2026-08-15"));
        assertThat(BudgetThresholdTrigger.period("MONTHLY", null, null, day), is("2026-08"));
        assertThat(BudgetThresholdTrigger.period("QUARTERLY", null, null, day), is("2026-Q3"));
        assertThat(BudgetThresholdTrigger.period("ANNUALLY", null, null, day), is("2026"));
        assertThat(BudgetThresholdTrigger.period("CUSTOM", "2026-01-01T00:00:00Z", "2026-06-30T00:00:00Z", day), is("2026-01-01T00:00:00Z/2026-06-30T00:00:00Z"));
    }

    @Test
    void givenTimeUnit_whenPeriodBounds_thenReturnsPeriodStartEndAndCappedGrace() {
        var day = LocalDate.of(2026, 8, 15);
        assertThat(BudgetThresholdTrigger.periodBounds("DAILY", null, null, day), is(bounds("2026-08-15T00:00:00Z", "2026-08-16T00:00:00Z")));
        assertThat(BudgetThresholdTrigger.periodBounds("MONTHLY", null, null, day), is(bounds("2026-08-01T00:00:00Z", "2026-09-01T00:00:00Z")));
        assertThat(BudgetThresholdTrigger.periodBounds("QUARTERLY", null, null, day), is(bounds("2026-07-01T00:00:00Z", "2026-10-01T00:00:00Z")));
        assertThat(BudgetThresholdTrigger.periodBounds("ANNUALLY", null, null, day), is(bounds("2026-01-01T00:00:00Z", "2027-01-01T00:00:00Z")));
        assertThat(
            BudgetThresholdTrigger.periodBounds("CUSTOM", "2026-01-01T00:00:00Z", "2026-06-30T00:00:00Z", day),
            is(bounds("2026-01-01T00:00:00Z", "2026-06-30T00:00:00Z"))
        );
        // without a parsable custom time period, the period start is unknown and no grace applies
        assertThat(BudgetThresholdTrigger.periodBounds("CUSTOM", "not-a-date", "2026-06-30T00:00:00Z", day), is(nullValue()));

        var daily = BudgetThresholdTrigger.periodBounds("DAILY", null, null, day);
        assertThat(BudgetThresholdTrigger.effectiveGrace(Duration.ofHours(12), daily), is(Duration.ofHours(6)));
        assertThat(BudgetThresholdTrigger.effectiveGrace(Duration.ofHours(2), daily), is(Duration.ofHours(2)));
        var quarterly = BudgetThresholdTrigger.periodBounds("QUARTERLY", null, null, day);
        assertThat(BudgetThresholdTrigger.effectiveGrace(Duration.ofDays(92), quarterly), is(Duration.ofDays(92).dividedBy(4)));
    }

    private static BudgetThresholdTrigger.PeriodBounds bounds(String start, String end) {
        return new BudgetThresholdTrigger.PeriodBounds(Instant.parse(start), Instant.parse(end));
    }

    private BudgetThresholdTrigger trigger(BudgetThresholdTrigger.ThresholdType type, double thresholdPercent, LocalDate today) {
        return spyToday(rawTrigger(type, thresholdPercent), today);
    }

    private BudgetThresholdTrigger rawTrigger(BudgetThresholdTrigger.ThresholdType type, double thresholdPercent) {
        return rawTrigger(type, thresholdPercent, BudgetThresholdTrigger.DEFAULT_PERIOD_START_GRACE);
    }

    private BudgetThresholdTrigger rawTrigger(BudgetThresholdTrigger.ThresholdType type, double thresholdPercent, Duration periodStartGrace) {
        return BudgetThresholdTrigger.builder()
            .id(IdUtils.create())
            .type(BudgetThresholdTrigger.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test"))
            .secretKeyId(Property.ofValue("test"))
            .endpointOverride(Property.ofValue(server.baseUrl()))
            .accountId(Property.ofValue("123456789012"))
            .budgetName(Property.ofValue("monthly-total"))
            .thresholdType(Property.ofValue(type))
            .thresholdPercent(Property.ofValue(thresholdPercent))
            .periodStartGrace(Property.ofValue(periodStartGrace))
            .build();
    }

    private static BudgetThresholdTrigger spyToday(BudgetThresholdTrigger trigger, LocalDate today) {
        return spyAt(trigger, today.atTime(12, 0).toInstant(ZoneOffset.UTC));
    }

    private static BudgetThresholdTrigger spyAt(BudgetThresholdTrigger trigger, Instant now) {
        var spy = spy(trigger);
        doReturn(now).when(spy).now();
        return spy;
    }

    private void stubBudget(String limit, String actual, String forecasted) {
        stubBudget("MONTHLY", limit, actual, forecasted);
    }

    private void stubBudget(String timeUnit, String limit, String actual, String forecasted) {
        server.stubFor(
            post("/").withHeader("X-Amz-Target", equalTo(DESCRIBE_BUDGET))
                .willReturn(okJson("""
                    {"Budget":{"BudgetName":"monthly-total","BudgetType":"COST","TimeUnit":"%s",
                      "BudgetLimit":{"Amount":"%s","Unit":"USD"},
                      "CalculatedSpend":{"ActualSpend":{"Amount":"%s","Unit":"USD"},"ForecastedSpend":{"Amount":"%s","Unit":"USD"}}}}
                    """.formatted(timeUnit, limit, actual, forecasted)))
        );
    }
}

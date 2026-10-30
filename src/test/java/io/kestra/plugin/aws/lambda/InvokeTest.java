package io.kestra.plugin.aws.lambda;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.models.executions.AbstractMetricEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import software.amazon.awssdk.core.exception.SdkClientException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InvokeTest extends AbstractInvokeTest {

    private RunContext context;

    @BeforeEach
    public void setUp() {
        this.context = runContextFactory.of();
    }

    @Test
    public void givenExistingLambda_whenInvoked_thenOutputOkMetricsOk() throws Exception {
        // Given
        var invoke = Invoke.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .functionArn(Property.ofValue(FUNCTION_NAME))
            .id(InvokeTest.class.getSimpleName())
            .type(InvokeTest.class.getName())
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        var output = invoke.run(context);

        // Then
        assertNotNull(output.getUri(), "File URI should be present");
        assertEquals(
            "application/json", output.getContentType(),
            "Output content type should be present"
        );
        assertTrue(output.getContentLength() > 10, "Output content length should have a value");
        assertTrue(
            context.metrics().stream().filter(m -> m.getName().equals("file.size"))
                .map(AbstractMetricEntry::getValue).findFirst().isPresent(),
            "Metric file.size should be present"
        );
        assertTrue(
            context.metrics().stream().filter(m -> m.getName().equals("duration"))
                .map(AbstractMetricEntry::getValue).findFirst().isPresent(),
            "Metric duration should be present"
        );
    }

    @Test
    public void givenNotFoundLambda_whenInvoked_thenErrorNoMetrics() throws Exception {
        // Given
        var invoke = Invoke.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .functionArn(Property.ofValue("Fake_ARN"))
            .id(InvokeTest.class.getSimpleName())
            .type(InvokeTest.class.getName())
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        assertThrows(
            LambdaInvokeException.class, () -> invoke.run(context),
            "Invocation should thrown an exception"
        );

        // Then
        assertTrue(context.metrics().isEmpty(), "Metrics should not be present");
    }

    @Test
    public void givenFailingLambda_whenInvoked_thenFailureNoMetrics() throws Exception {
        // Given
        Map<String, Object> params = new HashMap<>();
        // ask for an error in the Lambda by function param (see test resource lambda/test.py)
        params.put("action", "error");
        var invoke = Invoke.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .functionArn(Property.ofValue(FUNCTION_NAME))
            .functionPayload(Property.ofValue(params))
            .id(InvokeTest.class.getSimpleName())
            .type(InvokeTest.class.getName())
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        assertThrows(
            LambdaInvokeException.class, () -> invoke.run(context),
            "Invocation should fail"
        );

        // Then
        assertTrue(context.metrics().isEmpty(), "Metrics should not be present");
    }

    @Test
    public void givenSlowLambda_whenSocketTimeoutShorter_thenReadTimedOut() throws Exception {
        // Given
        var invoke = sleepingInvoke(5, Duration.ofSeconds(2)).build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        var e = assertThrows(
            SdkClientException.class, () -> invoke.run(context),
            "Invocation should time out on the client"
        );

        // Then
        assertTrue(e.getMessage().contains("Read timed out"), "Unexpected error: " + e.getMessage());
    }

    @Test
    public void givenSlowLambda_whenOneSdkAttempt_thenInvokedOnce() throws Exception {
        // Given
        var invoke = sleepingInvoke(5, Duration.ofSeconds(2))
            .sdkMaxAttempts(Property.ofValue(1))
            .build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        var e = assertThrows(
            SdkClientException.class, () -> invoke.run(context),
            "Invocation should time out on the client"
        );

        // Then
        assertTrue(e.getMessage().contains("(SDK Attempt Count: 1)"), "Unexpected error: " + e.getMessage());
    }

    @Test
    public void givenZeroSdkAttempts_whenClientBuilt_thenRejected() {
        // Given
        var invoke = sleepingInvoke(5, Duration.ofSeconds(2))
            .sdkMaxAttempts(Property.ofValue(0))
            .build();

        // When / Then
        assertThrows(
            IllegalArgumentException.class, () -> invoke.client(context),
            "sdkMaxAttempts below 1 should be rejected"
        );
    }

    @Test
    public void givenLambdaPastSdkDefault_whenSocketTimeoutLonger_thenOutputOk() throws Exception {
        // Given: longer than the SDK's 30 s default read timeout
        var invoke = sleepingInvoke(35, Duration.ofSeconds(60)).build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        var output = invoke.run(context);

        // Then
        assertNotNull(output.getUri(), "File URI should be present");
    }

    @Test
    public void givenSlowLambda_whenSocketTimeoutLonger_thenOutputOk() throws Exception {
        // Given
        var invoke = sleepingInvoke(5, Duration.ofSeconds(60)).build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        var output = invoke.run(context);

        // Then
        assertNotNull(output.getUri(), "File URI should be present");
    }

    private Invoke.InvokeBuilder<?, ?> sleepingInvoke(int seconds, Duration socketTimeout) {
        // ask the Lambda to sleep by function param (see test resource lambda/test.py)
        Map<String, Object> params = new HashMap<>();
        params.put("action", "sleep");
        params.put("seconds", seconds);
        return Invoke.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .functionArn(Property.ofValue(FUNCTION_NAME))
            .functionPayload(Property.ofValue(params))
            .socketTimeout(Property.ofValue(socketTimeout))
            .id(InvokeTest.class.getSimpleName())
            .type(InvokeTest.class.getName())
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY));
    }

}

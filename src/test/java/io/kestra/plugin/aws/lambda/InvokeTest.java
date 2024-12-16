package io.kestra.plugin.aws.lambda;

import io.kestra.core.models.executions.AbstractMetricEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.HashMap;
import java.util.Map;

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
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.LAMBDA).toString()))
            .functionArn(Property.of(FUNCTION_NAME))
            .id(InvokeTest.class.getSimpleName())
            .type(InvokeTest.class.getName())
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        var output = invoke.run(context);

        // Then
        assertNotNull(output.getUri(), "File URI should be present");
        assertEquals("text/plain; charset=UTF-8", output.getContentType(),
            "Output content type should be present");
        assertTrue(output.getContentLength() > 10, "Output content length should have a value");
        assertTrue(
            context.metrics().stream().filter(m -> m.getName().equals("file.size"))
                .map(AbstractMetricEntry::getValue).findFirst().isPresent(),
            "Metric file.size should be present");
        assertTrue(
            context.metrics().stream().filter(m -> m.getName().equals("duration"))
                .map(AbstractMetricEntry::getValue).findFirst().isPresent(),
            "Metric duration should be present");
    }

    @Test
    public void givenNotFoundLambda_whenInvoked_thenErrorNoMetrics() throws Exception {
        // Given
        var invoke = Invoke.builder()
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.LAMBDA).toString()))
            .functionArn(Property.of("Fake_ARN"))
            .id(InvokeTest.class.getSimpleName())
            .type(InvokeTest.class.getName())
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        assertThrows(LambdaInvokeException.class, () -> invoke.run(context),
            "Invocation should thrown an exception");

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
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.LAMBDA).toString()))
            .functionArn(Property.of(FUNCTION_NAME))
            .functionPayload(Property.of(params))
            .id(InvokeTest.class.getSimpleName())
            .type(InvokeTest.class.getName())
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        assertThrows(LambdaInvokeException.class, () -> invoke.run(context),
            "Invocation should fail");

        // Then
        assertTrue(context.metrics().isEmpty(), "Metrics should not be present");
    }

}

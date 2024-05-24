package io.kestra.plugin.aws.lambda;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.entity.ContentType;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

public class InvokeTest extends AbstractInvokeTest {

    private RunContext context;

    @BeforeEach
    public void setUp() throws IllegalVariableEvaluationException {
        this.context = runContextFactory.of();
    }

    @Ignore
    @Test
    public void givenExistingLambda_whenInvoked_thenOutputOkMetricsOk() throws Exception {
        // Given
        var invoke = Invoke.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.LAMBDA)
                        .toString())
                .functionArn(FUNCTION_NAME)
                .id(InvokeTest.class.getSimpleName())
                .type(InvokeTest.class.getName())
                .region(localstack.getRegion())
                .accessKeyId(localstack.getAccessKey())
                .secretKeyId(localstack.getSecretKey())
                .build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        var output = invoke.run(context);

        // Then
        assertNotNull(output.getUri(), "File URI should be present");
        assertEquals(ContentType.APPLICATION_JSON.getMimeType(), output.getContentType(),
                "Output content type should be present");
        assertTrue(output.getContentLength() > 10, "Output content length should have a value");
        assertTrue(
                context.metrics().stream().filter(m -> m.getName().equals("file.size"))
                        .map(m -> m.getValue()).findFirst().isPresent(),
                "Metric file.size should be present");
        assertTrue(
                context.metrics().stream().filter(m -> m.getName().equals("duration"))
                        .map(m -> m.getValue()).findFirst().isPresent(),
                "Metric duration should be present");
    }

    @Test
    public void givenNotFoundLambda_whenInvoked_thenErrorNoMetrics() throws Exception {
        // Given
        var invoke = Invoke.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.LAMBDA)
                        .toString())
                .functionArn("Fake_ARN")
                .id(InvokeTest.class.getSimpleName())
                .type(InvokeTest.class.getName())
                .region(localstack.getRegion())
                .accessKeyId(localstack.getAccessKey())
                .secretKeyId(localstack.getSecretKey())
                .build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        assertThrows(LambdaInvokeException.class, () -> invoke.run(context),
                "Invokation should thrown an exception");

        // Then
        assertTrue(context.metrics().size() == 0, "Metrics should not be present");
    }

    @Test
    public void givenFailingLambda_whenInvoked_thenFailureNoMetrics() throws Exception {
        // Given
        Map<String, Object> params = new HashMap<>();
        // ask for an error in the Lambda by function param (see test resource lambda/test.py)
        params.put("action", "error");
        var invoke = Invoke.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.LAMBDA)
                        .toString())
                .functionArn(FUNCTION_NAME).functionPayload(params)
                .id(InvokeTest.class.getSimpleName())
                .type(InvokeTest.class.getName())
                .region(localstack.getRegion())
                .accessKeyId(localstack.getAccessKey())
                .secretKeyId(localstack.getSecretKey())
                .build();

        var client = invoke.client(context);
        createFunction(client);

        // When
        assertThrows(LambdaInvokeException.class, () -> invoke.run(context),
                "Invokation should fail");

        // Then
        assertTrue(context.metrics().size() == 0, "Metrics should not be present");
    }

}

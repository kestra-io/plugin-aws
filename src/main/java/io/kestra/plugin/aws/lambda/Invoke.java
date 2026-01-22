package io.kestra.plugin.aws.lambda;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.retrys.AbstractRetry;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.RetryUtils;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.kestra.plugin.aws.cloudwatch.CloudWatchLogs;
import io.kestra.plugin.aws.lambda.Invoke.Output;
import io.kestra.plugin.aws.s3.ObjectOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilterLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilteredLogEvent;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;
import io.kestra.core.models.tasks.retrys.Exponential;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Invoke an AWS Lambda function."
)
@Plugin(
    examples = {
        @Example(
            title = "Invoke given Lambda function and wait for its completion.",
            full = true,
            code = """
                id: aws_lambda_invoke
                namespace: company.team

                tasks:
                  - id: invoke
                    type: io.kestra.plugin.aws.lambda.Invoke
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    functionArn: "arn:aws:lambda:eu-central-1:123456789012:function:my-function"
                """
        ),
        @Example(
            title = "Invoke given Lambda function with given payload parameters and wait for its completion. Payload is a map of items.",
            full = true,
            code = """
                id: aws_lambda_invoke
                namespace: company.team

                tasks:
                  - id: invoke
                    type: io.kestra.plugin.aws.lambda.Invoke
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    functionArn: "arn:aws:lambda:eu-central-1:123456789012:function:my-function"
                    functionPayload:
                        id: 1
                        firstname: "John"
                        lastname: "Doe"
                """
        )
    },
    metrics = {
        @Metric(name = "file.size", type = Counter.TYPE),
        @Metric(name = "duration", type = Timer.TYPE)
    }
)

@Slf4j
public class Invoke extends AbstractConnection implements RunnableTask<Output> {

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();

    @Schema(title = "The Lambda function name.")
    @NotNull
    private Property<String> functionArn;

    @Schema(title = "Function request payload.", description = "Request payload. It's a map of string -> object.")
    private Property<Map<String, Object>> functionPayload;

    @Override
    public Output run(RunContext runContext) throws Exception {
        final long start = System.nanoTime();
        final Instant invocationStart = Instant.now().minusSeconds(5);
        var functionArn = runContext.render(this.functionArn).as(String.class).orElseThrow();
        var requestPayload = runContext.render(this.functionPayload).asMap(String.class, Object.class).isEmpty() ? null
                : runContext.render(this.functionPayload).asMap(String.class, Object.class);
        var logger = runContext.logger();

        try (var lambda = client(runContext)) {
            var builder = InvokeRequest.builder().functionName(functionArn);
            if (requestPayload != null && requestPayload.size() > 0) {
                var payload = SdkBytes.fromUtf8String(OBJECT_MAPPER.writeValueAsString(requestPayload));
                builder.payload(payload);
            }
            InvokeRequest request = builder.build();
            // TODO take care about long-running functions: your client might disconnect during
            // synchronous invocation while it waits for a response. Configure your HTTP client,
            // SDK, firewall, proxy, or operating system to allow for long connections with timeout
            InvokeResponse res = lambda.invoke(request);
            Optional<String> contentTypeHeader = res.sdkHttpResponse().firstMatchingHeader(HttpHeaders.CONTENT_TYPE);
            ContentType contentType = parseContentType(contentTypeHeader);
            if (res.functionError() != null) {
                // If error response, then detect the message, and construct an exception, log
                // details and throw - this will throw an exception in any case
                handleError(functionArn, contentType, res.payload());
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Lambda {} invoked successfully", functionArn);
            }
            Output out = handleContent(runContext, functionArn, contentType, res.payload());
            fetchAndLogLambdaLogs(runContext, functionArn, invocationStart);
            runContext.metric(Timer.of("duration", Duration.ofNanos(System.nanoTime() - start)));
            return out;
        } catch (LambdaException e) {
            throw new LambdaInvokeException("Lambda Invoke task execution failed for function: " + functionArn, e);
        }
    }

    @VisibleForTesting
    CloudWatchLogsClient getCloudWatchLogsClient(RunContext runContext) throws IllegalVariableEvaluationException {
        return new CloudWatchLogs().logsClient(runContext);
    }

    @VisibleForTesting
    LambdaClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, LambdaClient.builder()).build();
    }

    @VisibleForTesting
    ContentType parseContentType(Optional<String> contentType) {
        if (contentType.isPresent()) {
            try {
                // Parse and construct content type reflecting the given header value
                var parsed = ContentType.parse(contentType.get());
                var known = ContentType.getByMimeType(parsed.getMimeType());
                if (known != null) {
                    // Apply charset only if it was provided originally
                    return ContentType.create(known.getMimeType(), parsed.getCharset());
                }
            } catch (Exception cte) {
                log.warn("Unable to parse Lambda response content type {}: {}", contentType.get(), cte.getMessage());
            }
        }
        // By default it's binary type
        return ContentType.APPLICATION_OCTET_STREAM;
    }

    @VisibleForTesting
    Optional<String> readError(String payload) {
        try {
            // Sample error from AWS could be:
            // {"errorMessage": "'path'", "errorType": "KeyError", "requestId":
            // "f32ff4cf-b0dc-44ec-a59a-4c5b18b836c3", "stackTrace": [" File \"/var/task/hello.py\",
            // line 12, in handler\n \"body\": \"Hello AWS Lambda!!! You have requested
            // {}\".format(event[\"path\"])\n"]}
            // TODO May be it's more resonable to return the whole payload as an error
            // but then there are risks:
            // 1) to expose something sensitive to unexpected readers
            // 2) to load too large string and cause memory/perf consumption w/o a reason
            final var message = OBJECT_MAPPER.readTree(payload).path("errorMessage");
            if (message.isValueNode()) {
                return Optional.of(message.asText());
            }
        } catch (JsonProcessingException e) {
            log.warn("Unable to read Lambda error response JSON: {}", e.getMessage());
        }
        return Optional.empty();
    }

    @VisibleForTesting
    void fetchAndLogLambdaLogs(RunContext runContext, String functionArn, Instant startTime) {
        var logger = runContext.logger();
        String functionName = extractFunctionName(functionArn);
        String logGroupName = "/aws/lambda/" + functionName;

        // Explicit retry policy: 5 attempts, 3s interval, maxInterval 3s
        AbstractRetry retryPolicy = Exponential.builder()
                .interval(Duration.ofSeconds(3))
                .maxAttempts(5)
                .maxInterval(Duration.ofSeconds(10))
                .build();

        try (CloudWatchLogsClient logsClient = getCloudWatchLogsClient(runContext)) {
            // Explicitly specify generic type for RetryUtils
            List<FilteredLogEvent> events = RetryUtils.<List<FilteredLogEvent>, Exception>of(retryPolicy, logger)
                    .run(
                            result -> result == null || result.isEmpty(),
                            () -> {
                                FilterLogEventsRequest request = FilterLogEventsRequest.builder()
                                        .logGroupName(logGroupName)
                                        .startTime(startTime.toEpochMilli())
                                        .build();

                                var response = logsClient.filterLogEvents(request);
                                return response.events();
                            });

            if (events != null) {
                events.forEach(event -> logger.info("[lambda] {}", event.message().trim()));
            }

        } catch (Exception e) {
            logger.warn("Failed to fetch CloudWatch logs for Lambda {}: {}", functionArn, e.getMessage());
        }
    }

    @VisibleForTesting
    private String extractFunctionName(String functionArnOrName) {
        if (functionArnOrName.contains(":function:")) {
            // Handle Full ARN
            return functionArnOrName.split(":function:")[1].split(":")[0];
        }
        // Handle just the name
        return functionArnOrName;
    }

    @VisibleForTesting
    void handleError(String functionArn, ContentType contentType, SdkBytes payload) {
        String errorPayload;
        try {
            errorPayload = payload.asUtf8String();
        } catch (UncheckedIOException e) {
            log.warn("Lambda function response payload cannot be read as UTF8 string: {}",
                    e.getMessage());
            errorPayload = null;
        }
        if (log.isDebugEnabled()) {
            log.debug("Lambda function error for {}: response type: {}, response payload: {}",
                    functionArn, contentType, errorPayload);
        }
        if (errorPayload != null
                && ContentType.APPLICATION_JSON.getMimeType().equals(contentType.getMimeType())) {
            throw new LambdaInvokeException(
                    "Lambda Invoke task responded with error for function: " + functionArn
                            + ". Error: " + errorPayload);
        } else {
            throw new LambdaInvokeException(
                    "Lambda Invoke task responded with error for function: " + functionArn);
        }
    }

    @VisibleForTesting
    Output handleContent(RunContext runContext, String functionArn, ContentType contentType,
            SdkBytes payload) {
        var logger = runContext.logger();
        try (var dataStream = payload.asInputStream()) {
            File tempFile = runContext.workingDir().createTempFile().toFile();
            // noinspection ResultOfMethodCallIgnored
            tempFile.delete();

            var size = Files.copy(dataStream, tempFile.toPath());
            // Doing the same as in S3Service.download()
            runContext.metric(Counter.of("file.size", size));
            var uri = runContext.storage().putFile(tempFile);
            if (logger.isDebugEnabled()) {
                logger.debug("Lambda invokation task completed {}: response type: {}, file: `{}",
                        functionArn, contentType, uri);
            }
            return Output.builder()
                    .uri(uri)
                    .contentLength(size)
                    .contentType(contentType.toString()).build();
        } catch (IOException e) {
            throw new LambdaInvokeException(
                    "Lambda Invoke task failed to read data for function: " + functionArn, e);
        }
    }

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Response file URI.")
        private final URI uri;

        @Schema(title = "Size of the response content in bytes.")

        private final Long contentLength;

        @Schema(title = "A standard MIME type describing the format of the content.")
        private final String contentType;
    }
}

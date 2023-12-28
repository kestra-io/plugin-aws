package io.kestra.plugin.aws.lambda;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.lambda.Invoke.Output;
import io.kestra.plugin.aws.s3.ObjectOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Invoke Lambda function and wait for its completion.")
@Plugin(
    examples = {
        @Example(
            title = "Invoke given Lambda function and wait for its completion.",
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "functionArn: \"arn:aws:lambda:us-west-2:123456789012:function:my-function\""
            }
        ),
        @Example(
            title = "Invoke given Lambda function with given payload parameters and wait for its completion. Payload is a map of items.",
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "functionArn: \"arn:aws:lambda:us-west-2:123456789012:function:my-function\"",
                "functionPayload:",
                "    id: 1",
                "    firstname: \"John\"",
                "    lastname: \"Doe\""
            }
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
    @PluginProperty(dynamic = true)
    @NotNull
    private String functionArn;

    @Schema(
        title = "Function request payload.",
        description = "Request payload. It's a map of string -> object."
    )    
    @PluginProperty(dynamic = true)
    private Map<String, Object> functionPayload;

    @Override
    public Output run(RunContext runContext) throws Exception {
        final long start = System.nanoTime();
        var functionArn = runContext.render(this.functionArn);
        var requestPayload = this.functionPayload != null ? runContext.render(this.functionPayload) : null;
        try (var lambda = client(runContext)) {
            var builder = InvokeRequest.builder().functionName(functionArn);
            if (requestPayload != null && requestPayload.size() > 0) {
                var payload = SdkBytes.fromUtf8String(OBJECT_MAPPER.writeValueAsString(requestPayload)) ;
                builder.payload(payload);
            }
            InvokeRequest request = builder.build();
            // TODO take care about long-running functions: your client might disconnect during
            // synchronous invocation while it waits for a response. Configure your HTTP client,
            // SDK, firewall, proxy, or operating system to allow for long connections with timeout
            // or keep-alive settings.
            InvokeResponse res = lambda.invoke(request);
            Optional<String> contentTypeHeader = res.sdkHttpResponse().firstMatchingHeader(HttpHeaders.CONTENT_TYPE);
            ContentType contentType = parseContentType(contentTypeHeader);
            if (res.functionError() != null) {
                // If error response, then detect the message, and construct an exception, log
                // details and throw - this will throw an exception in any case
                handleError(functionArn, contentType, res.payload());
            }
            if (log.isDebugEnabled()) {
                log.debug("Lambda {} invoked successfully", functionArn);
            }
            Output out = handleContent(runContext, functionArn, contentType, res.payload());
            runContext.metric(Timer.of("duration", Duration.ofNanos(System.nanoTime() - start)));
            return out;
        } catch (LambdaException e) {
            throw new LambdaInvokeException("Lambda Invoke task execution failed for function: " + functionArn, e);
        }
    }

    @VisibleForTesting
    LambdaClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        LambdaClientBuilder builder = LambdaClient.builder().httpClient(ApacheHttpClient.create())
                .credentialsProvider(this.credentials(runContext));

        if (this.region != null) {
            builder.region(Region.of(runContext.render(this.region)));
        }
        if (this.endpointOverride != null) {
            builder.endpointOverride(URI.create(runContext.render(this.endpointOverride)));
        }

        return builder.build();
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
            } catch(Exception cte) {
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
    void handleError(String functionArn, ContentType contentType, SdkBytes payload) {
        String errorPayload;
        try {
            errorPayload = payload.asUtf8String();
        } catch (UncheckedIOException e) {
            log.warn("Lambda function respone payload cannot be read as UTF8 string: {}",
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
        try (var dataStream = payload.asInputStream()) {
            File tempFile = runContext.tempFile().toFile();
            // noinspection ResultOfMethodCallIgnored
            tempFile.delete();

            var size = Files.copy(dataStream, tempFile.toPath());
            // Doing the same as in S3Service.download()
            runContext.metric(Counter.of("file.size", size));
            var uri = runContext.putTempFile(tempFile);
            if (log.isDebugEnabled()) {
                log.debug("Lambda invokation task completed {}: response type: {}, file: `{}", 
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

        @Schema(
            title = "Response file URI."
        )
        private final URI uri;

        @Schema(
            title = "Size of the response content in bytes."
        )

        private final Long contentLength;

        @Schema(
            title = "A standard MIME type describing the format of the content."
        )
        private final String contentType;
    }
}

package io.kestra.plugin.aws.lambda;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;
import org.apache.http.Consts;
import org.apache.http.entity.ContentType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.lambda.Invoke.Output;
import io.kestra.plugin.aws.s3.ObjectOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.val;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.model.InvocationType;
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
                "functionArn: \"arn:aws:lambda:us-west-2:123456789012:function:my-function\""
            }
        ),
        @Example(
            title = "Invoke given Lambda function with given payload parameters and wait for its completion. Payload is a map of items.",
            code = {
                "functionArn: \"arn:aws:lambda:us-west-2:123456789012:function:my-function\"",
                "functionPayload:",
                "  id: 1",
                "  firstname: \"John\"",
                "  lastname: \"Doe\""
            }
        )
    }
)
@Slf4j
public class Invoke extends AbstractLambdaInvoke implements RunnableTask<Output> {

    @Schema(
        title = "Function request payload.",
        description = "Request payload. It's a map of string -> object."
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> functionPayload;

    @Override
    public Output run(RunContext runContext) throws Exception {
        val functionArn = runContext.render(this.functionArn);
        val requestPayload = this.functionPayload != null ? runContext.render(this.functionPayload) : null;
        try (val lambda = client(runContext)) {
            val builder = InvokeRequest.builder().functionName(functionArn);
            if (requestPayload != null && requestPayload.size() > 0) {
                // TODO add input parameters, currently from the flow (provided by the user)
                // But next, it could a result from previous task(s)
                // builder.payload(payload)
            }
            if (!wait) {
                // This should not work until we'll decide to suppprt async requests
                builder.invocationType(InvocationType.EVENT);
            }            
            InvokeRequest request = builder.build();
            // TODO take care about long-running functions: your client might disconnect during
            // synchronous invocation while it waits for a response. Configure your HTTP client,
            // SDK, firewall, proxy, or operating system to allow for long connections with timeout
            // or keep-alive settings.
            InvokeResponse res = lambda.invoke(request);
            // Check mimetype of the payload and respectivelly save in temp and then store
            Optional<String> contentTypeHeader = res.sdkHttpResponse().firstMatchingHeader("Content-Type");
            ContentType contentType = parseContentType(contentTypeHeader);
            if (res.functionError() != null) {
                // If error response, then detect the message, and construct an exception, log
                // details and throw
                // This will throw an exception in any case
                handleError(functionArn, contentType, res.payload());
            }
            if (log.isDebugEnabled()) {
                log.debug("Lambda {} invoked successfully", functionArn);
            }
            return handleContent(runContext, functionArn, contentType, res.payload());
        } catch (LambdaException e) {
            throw new LambdaInvokeException("Lambda Invoke task execution failed for function: " + functionArn, e);
        }
    }

    ContentType parseContentType(Optional<String> contentType) {
        if (contentType.isPresent()) {
            try {
                val ct = ContentType.getByMimeType(ContentType.parse(contentType.get()).getMimeType());
                if (ct !=null) {
                    return ct;
                }
            } catch(Exception cte) {
                log.warn("Unable to parse Lambda response content type {}: {}", contentType.get(), cte.getMessage());
            }
        }
        // By default it's binary type
        return ContentType.APPLICATION_OCTET_STREAM;
    }

    Optional<String> readError(String payload) {
        ObjectMapper jsonMapper = new ObjectMapper();
        try {
            // TODO May be it's more resonable to return the whole payload as an error
            // but then there are risks:
            // 1) to expose something sensitive to unexpected readers
            // 2) to load too large string and cause memory/perf consumption w/o a reason
            val message = jsonMapper.readTree(payload).path("errorMessage");
            if (message.isValueNode()) {
                return Optional.of(message.asText());
            }
        } catch(JsonProcessingException e) {
            log.warn("Unable to read Lambda error response JSON: {}", e.getMessage());
        }
        return Optional.empty();
    }

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
                && ContentType.APPLICATION_JSON.getMimeType().equals(contentType.getMimeType())
                && (contentType.getCharset() == null || Consts.UTF_8.equals(contentType.getCharset()))) {
            val errorMessage = readError(errorPayload);
            throw new LambdaInvokeException(
                    "Lambda Invoke task responded with error for function: " + functionArn
                            + (errorMessage.isPresent() ? ". Error: " + errorMessage.get() : ""));
        } else {
            throw new LambdaInvokeException(
                    "Lambda Invoke task responded with error for function: " + functionArn);
        }
    }

    Output handleContent(RunContext runContext, String functionArn, ContentType contentType,
            SdkBytes payload) {
        try (val dataStream = payload.asInputStream()) {
            File tempFile = runContext.tempFile("lambdainvoke").toFile();
            // noinspection ResultOfMethodCallIgnored
            tempFile.delete();

            val size = Files.copy(dataStream, tempFile.toPath());
            // Doing the same as in S3Service.download()
            runContext.metric(Counter.of("file.size", size));
            val uri = runContext.putTempFile(tempFile);
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

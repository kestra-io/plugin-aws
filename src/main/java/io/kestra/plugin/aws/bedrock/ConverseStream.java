package io.kestra.plugin.aws.bedrock;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Stream a conversation with an Amazon Bedrock foundation model",
    description = "Streaming variant of the Converse task. Accumulates all streamed tokens and returns the complete " +
        "assistant message once generation finishes. Use this when you want lower time-to-first-token latency for long responses."
)
@Plugin(
    examples = {
        @Example(
            title = "Stream a response from Claude on Bedrock and log the accumulated reply.",
            full = true,
            code = """
                id: bedrock_converse_stream
                namespace: company.team

                inputs:
                  - id: user_message
                    type: STRING

                tasks:
                  - id: stream_chat
                    type: io.kestra.plugin.aws.bedrock.ConverseStream
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    modelId: "anthropic.claude-3-5-sonnet-20241022-v2:0"
                    messages:
                      - role: user
                        content: "{{ inputs.user_message }}"
                    system: "You are a concise data engineering assistant."
                    inferenceConfig:
                      maxTokens: 2048

                  - id: log_reply
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.stream_chat.content }}"
                """
        )
    }
)
public class ConverseStream extends AbstractConnection implements RunnableTask<ConverseStream.Output> {

    @Schema(
        title = "Model ID",
        description = "The Bedrock model identifier, e.g. `anthropic.claude-3-5-sonnet-20241022-v2:0`."
    )
    @NotNull
    private Property<String> modelId;

    @Schema(
        title = "Messages",
        description = "Ordered list of conversation turns. Each entry must have a `role` (`user` or `assistant`) and `content` (text string)."
    )
    @NotEmpty
    private Property<List<Map<String, String>>> messages;

    @Schema(
        title = "System prompt",
        description = "Optional system-level instruction prepended before the conversation."
    )
    private Property<String> system;

    @Schema(
        title = "Inference configuration",
        description = "Optional inference parameters. Supported keys: `maxTokens` (integer), `temperature` (float 0–1), `topP` (float 0–1), `stopSequences` (list of strings)."
    )
    private Property<Map<String, Object>> inferenceConfig;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        String resolvedModelId = runContext.render(this.modelId).as(String.class).orElseThrow();

        @SuppressWarnings("unchecked")
        List<Map<String, String>> rawMessages = (List<Map<String, String>>) (List<?>)
            runContext.render(this.messages).asList(Map.class);

        List<Message> sdkMessages = BedrockUtils.buildMessages(rawMessages);

        ConverseStreamRequest.Builder requestBuilder = ConverseStreamRequest.builder()
            .modelId(resolvedModelId)
            .messages(sdkMessages);

        if (this.system != null) {
            String resolvedSystem = runContext.render(this.system).as(String.class).orElse(null);
            if (resolvedSystem != null && !resolvedSystem.isBlank()) {
                requestBuilder.system(SystemContentBlock.fromText(resolvedSystem));
            }
        }

        if (this.inferenceConfig != null) {
            Map<String, Object> ic = runContext.render(this.inferenceConfig).asMap(String.class, Object.class);
            requestBuilder.inferenceConfig(BedrockUtils.buildInferenceConfig(ic));
        }

        logger.debug("Starting Bedrock ConverseStream for model '{}'", resolvedModelId);

        // StringBuffer for thread-safe appends from the SDK event subscriber thread.
        StringBuffer accumulated = new StringBuffer();
        AtomicReference<String> stopReason = new AtomicReference<>();
        AtomicInteger inputTokens = new AtomicInteger();
        AtomicInteger outputTokens = new AtomicInteger();

        try (BedrockRuntimeAsyncClient client = asyncClient(runContext)) {
            ConverseStreamResponseHandler handler = ConverseStreamResponseHandler.builder()
                .onEventStream(publisher -> publisher.subscribe(event -> {
                    if (event instanceof ContentBlockDeltaEvent delta) {
                        ContentBlockDelta d = delta.delta();
                        if (d != null && d.text() != null) {
                            accumulated.append(d.text());
                        }
                    } else if (event instanceof MessageStopEvent stop) {
                        stopReason.set(stop.stopReason() != null ? stop.stopReason().toString() : null);
                    } else if (event instanceof ConverseStreamMetadataEvent meta) {
                        TokenUsage usage = meta.usage();
                        if (usage != null) {
                            inputTokens.set(usage.inputTokens() != null ? usage.inputTokens() : 0);
                            outputTokens.set(usage.outputTokens() != null ? usage.outputTokens() : 0);
                        }
                    }
                }))
                .build();

            CompletableFuture<Void> future = client.converseStream(requestBuilder.build(), handler);
            // Use get() instead of join() so the original BedrockRuntimeException is not wrapped
            // in CompletionException and propagates cleanly to Kestra's error reporting.
            try {
                future.get();
            } catch (ExecutionException e) {
                throw (Exception) e.getCause();
            }
        }

        String content = accumulated.toString();
        logger.debug("ConverseStream completed. stopReason={}, inputTokens={}, outputTokens={}, chars={}",
            stopReason.get(), inputTokens.get(), outputTokens.get(), content.length());

        return Output.builder()
            .modelId(resolvedModelId)
            .content(content)
            .stopReason(stopReason.get())
            .inputTokens(inputTokens.get())
            .outputTokens(outputTokens.get())
            .build();
    }

    @VisibleForTesting
    BedrockRuntimeAsyncClient asyncClient(RunContext runContext) throws Exception {
        var clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureAsyncClient(clientConfig, BedrockRuntimeAsyncClient.builder()).build();
    }

    @SuperBuilder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Model ID", description = "The model that was invoked.")
        private final String modelId;

        @Schema(title = "Content", description = "The fully accumulated assistant reply.")
        private final String content;

        @Schema(title = "Stop reason", description = "Why the model stopped generating tokens.")
        private final String stopReason;

        @Schema(title = "Input tokens", description = "Number of input tokens consumed.")
        private final Integer inputTokens;

        @Schema(title = "Output tokens", description = "Number of output tokens generated.")
        private final Integer outputTokens;
    }
}

package io.kestra.plugin.aws.bedrock;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Converse with an Amazon Bedrock foundation model",
    description = "Sends a structured multi-turn conversation to any Bedrock model using the unified Converse API. " +
        "Supports system prompts, multi-turn message history, and inference configuration such as max tokens and temperature."
)
@Plugin(
    examples = {
        @Example(
            title = "Structured chat with Claude via the Bedrock Converse API.",
            full = true,
            code = """
                id: bedrock_converse
                namespace: company.team

                inputs:
                  - id: user_message
                    type: STRING

                tasks:
                  - id: chat
                    type: io.kestra.plugin.aws.bedrock.Converse
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    modelId: "anthropic.claude-3-5-sonnet-20241022-v2:0"
                    messages:
                      - role: user
                        content: "{{ inputs.user_message }}"
                    system: "You are a concise data engineering assistant."
                    inferenceConfig:
                      maxTokens: 1024

                  - id: log_reply
                    type: io.kestra.plugin.core.log.Log
                    message: "Assistant: {{ outputs.chat.content }}"
                """
        )
    }
)
public class Converse extends AbstractConnection implements RunnableTask<Converse.Output> {

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

        List<Map<String, String>> resolvedMessages = runContext.render(this.messages)
            .asList(Map.class)
            .stream()
            .map(m -> {
                @SuppressWarnings("unchecked")
                Map<String, String> entry = (Map<String, String>) m;
                return entry;
            })
            .toList();

        List<Message> sdkMessages = new ArrayList<>();
        for (Map<String, String> msg : resolvedMessages) {
            String role = msg.get("role");
            String content = msg.get("content");
            ConversationRole sdkRole = "assistant".equalsIgnoreCase(role)
                ? ConversationRole.ASSISTANT
                : ConversationRole.USER;
            sdkMessages.add(Message.builder()
                .role(sdkRole)
                .content(ContentBlock.fromText(content))
                .build());
        }

        ConverseRequest.Builder requestBuilder = ConverseRequest.builder()
            .modelId(resolvedModelId)
            .messages(sdkMessages);

        // System prompt
        if (this.system != null) {
            String resolvedSystem = runContext.render(this.system).as(String.class).orElse(null);
            if (resolvedSystem != null && !resolvedSystem.isBlank()) {
                requestBuilder.system(SystemContentBlock.fromText(resolvedSystem));
            }
        }

        // Inference config
        if (this.inferenceConfig != null) {
            Map<String, Object> ic = runContext.render(this.inferenceConfig).asMap(String.class, Object.class);
            InferenceConfiguration.Builder icBuilder = InferenceConfiguration.builder();
            if (ic.containsKey("maxTokens")) {
                icBuilder.maxTokens(((Number) ic.get("maxTokens")).intValue());
            }
            if (ic.containsKey("temperature")) {
                icBuilder.temperature(((Number) ic.get("temperature")).floatValue());
            }
            if (ic.containsKey("topP")) {
                icBuilder.topP(((Number) ic.get("topP")).floatValue());
            }
            if (ic.containsKey("stopSequences")) {
                @SuppressWarnings("unchecked")
                List<String> stops = (List<String>) ic.get("stopSequences");
                icBuilder.stopSequences(stops);
            }
            requestBuilder.inferenceConfig(icBuilder.build());
        }

        logger.debug("Invoking Bedrock Converse for model '{}'", resolvedModelId);

        try (BedrockRuntimeClient client = client(runContext)) {
            ConverseResponse response = client.converse(requestBuilder.build());
            Message outputMessage = response.output().message();

            String content = outputMessage.content().stream()
                .filter(cb -> cb.text() != null)
                .map(ContentBlock::text)
                .findFirst()
                .orElse("");

            String stopReason = response.stopReason() != null ? response.stopReason().toString() : null;

            TokenUsage usage = response.usage();
            Integer inputTokens = usage != null ? usage.inputTokens() : null;
            Integer outputTokens = usage != null ? usage.outputTokens() : null;

            logger.debug("Converse completed. stopReason={}, inputTokens={}, outputTokens={}", stopReason, inputTokens, outputTokens);

            return Output.builder()
                .modelId(resolvedModelId)
                .content(content)
                .stopReason(stopReason)
                .inputTokens(inputTokens)
                .outputTokens(outputTokens)
                .build();
        }
    }

    BedrockRuntimeClient client(RunContext runContext) throws Exception {
        var clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, BedrockRuntimeClient.builder()).build();
    }

    @SuperBuilder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Model ID", description = "The model that was invoked.")
        private final String modelId;

        @Schema(title = "Content", description = "The assistant's text reply.")
        private final String content;

        @Schema(title = "Stop reason", description = "Why the model stopped generating tokens, e.g. `end_turn` or `max_tokens`.")
        private final String stopReason;

        @Schema(title = "Input tokens", description = "Number of input tokens consumed.")
        private final Integer inputTokens;

        @Schema(title = "Output tokens", description = "Number of output tokens generated.")
        private final Integer outputTokens;
    }
}

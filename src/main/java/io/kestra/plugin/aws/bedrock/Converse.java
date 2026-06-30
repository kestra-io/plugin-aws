package io.kestra.plugin.aws.bedrock;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Converse with an Amazon Bedrock foundation model",
    description = """
        Sends a structured multi-turn conversation to any Bedrock model using the unified Converse API.
        Supports system prompts, multi-turn message history, and inference configuration such as max tokens and temperature.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Structured chat with Claude via the Bedrock Converse API",
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
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> modelId;

    @Schema(
        title = "Messages",
        description = "Ordered list of conversation turns. Each entry must have a `role` (`user` or `assistant`) and `content` (text string)."
    )
    @PluginProperty(group = "main")
    @NotEmpty
    private Property<List<Map<String, String>>> messages;

    @Schema(
        title = "System prompt",
        description = "Optional system-level instruction prepended before the conversation."
    )
    @PluginProperty(group = "processing")
    private Property<String> system;

    @Schema(
        title = "Inference configuration",
        description = "Optional inference parameters. Supported keys: `maxTokens` (integer), `temperature` (float 0–1), `topP` (float 0–1), `stopSequences` (list of strings)."
    )
    @PluginProperty(group = "processing")
    private Property<Map<String, Object>> inferenceConfig;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var resolvedModelId = runContext.render(this.modelId).as(String.class).orElseThrow();

        List<Map<String, String>> rawMessages;
        try {
            @SuppressWarnings("unchecked")
            var cast = (List<Map<String, String>>) (List<?>) runContext.render(this.messages).asList(Map.class);
            rawMessages = cast;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("messages must be a list of {role, content} maps", e);
        }

        var sdkMessages = BedrockUtils.buildMessages(rawMessages);

        var requestBuilder = ConverseRequest.builder()
            .modelId(resolvedModelId)
            .messages(sdkMessages);

        if (this.system != null) {
            var resolvedSystem = runContext.render(this.system).as(String.class).orElse(null);
            if (resolvedSystem != null && !resolvedSystem.isBlank()) {
                requestBuilder.system(SystemContentBlock.fromText(resolvedSystem));
            }
        }

        if (this.inferenceConfig != null) {
            var ic = runContext.render(this.inferenceConfig).asMap(String.class, Object.class);
            requestBuilder.inferenceConfig(BedrockUtils.buildInferenceConfig(ic));
        }

        logger.debug("Invoking Bedrock Converse for model '{}'", resolvedModelId);

        try (var client = client(runContext)) {
            var response = client.converse(requestBuilder.build());
            var outputMessage = response.output().message();

            var content = outputMessage.content().stream()
                .filter(cb -> cb.text() != null)
                .map(ContentBlock::text)
                .findFirst()
                .orElse("");

            var stopReason = response.stopReason() != null ? response.stopReason().toString() : null;
            var usage = response.usage();
            var inputTokens = usage != null ? usage.inputTokens() : null;
            var outputTokens = usage != null ? usage.outputTokens() : null;

            logger.debug(
                "Converse completed. stopReason={}, inputTokens={}, outputTokens={}",
                stopReason, inputTokens, outputTokens
            );

            return Output.builder()
                .modelId(resolvedModelId)
                .content(content)
                .stopReason(stopReason)
                .inputTokens(inputTokens)
                .outputTokens(outputTokens)
                .build();
        }
    }

    @VisibleForTesting
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

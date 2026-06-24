package io.kestra.plugin.aws.bedrock;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Invoke an Amazon Bedrock foundation model",
    description = "Sends a raw JSON payload to any Bedrock-supported foundation model and returns the parsed response body. " +
        "The caller is responsible for providing a `body` that matches the model's own request schema."
)
@Plugin(
    examples = {
        @Example(
            title = "Generate text with Amazon Titan via InvokeModel.",
            full = true,
            code = """
                id: bedrock_invoke_model
                namespace: company.team

                inputs:
                  - id: prompt
                    type: STRING
                    defaults: "Summarize the key benefits of Apache Kafka in 3 bullet points."

                tasks:
                  - id: invoke_titan
                    type: io.kestra.plugin.aws.bedrock.InvokeModel
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    modelId: "amazon.titan-text-express-v1"
                    body:
                      inputText: "{{ inputs.prompt }}"
                      textGenerationConfig:
                        maxTokenCount: 512
                        temperature: 0.7

                  - id: log_response
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.invoke_titan.body }}"
                """
        )
    }
)
public class InvokeModel extends AbstractConnection implements RunnableTask<InvokeModel.Output> {

    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    @Schema(
        title = "Model ID",
        description = "The Bedrock model identifier, e.g. `amazon.titan-text-express-v1` or `anthropic.claude-3-5-sonnet-20241022-v2:0`."
    )
    @NotNull
    private Property<String> modelId;

    @Schema(
        title = "Request body",
        description = "Model-specific request payload as a map. The structure must match the target model's API schema."
    )
    @NotNull
    private Property<Map<String, Object>> body;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        String resolvedModelId = runContext.render(this.modelId).as(String.class).orElseThrow();
        Map<String, Object> resolvedBody = runContext.render(this.body).asMap(String.class, Object.class);

        String requestJson = MAPPER.writeValueAsString(resolvedBody);
        // Log only model ID and byte length — never the body content, which may contain PII.
        logger.debug("Invoking Bedrock model '{}', request size={} bytes", resolvedModelId, requestJson.length());

        try (BedrockRuntimeClient client = client(runContext)) {
            InvokeModelRequest request = InvokeModelRequest.builder()
                .modelId(resolvedModelId)
                .contentType("application/json")
                .accept("application/json")
                .body(SdkBytes.fromUtf8String(requestJson))
                .build();

            InvokeModelResponse response = client.invokeModel(request);
            String responseBody = response.body().asUtf8String();
            // Log only byte length — response may contain confidential business data.
            logger.debug("Bedrock model '{}' responded, response size={} bytes", resolvedModelId, responseBody.length());

            Map<String, Object> parsedBody = MAPPER.readValue(responseBody, MAP_TYPE);

            return Output.builder()
                .modelId(resolvedModelId)
                .body(parsedBody)
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

        @Schema(title = "Response body", description = "Parsed JSON response body returned by the model.")
        private final Map<String, Object> body;
    }
}

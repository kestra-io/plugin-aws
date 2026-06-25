package io.kestra.plugin.aws.bedrock;

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
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.bedrock.BedrockClient;
import software.amazon.awssdk.services.bedrock.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List available Amazon Bedrock foundation models",
    description = """
        Returns the foundation models available in the configured AWS region,
        optionally filtered by provider, output modality, or inference type.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Discover available text-output models and log their IDs.",
            full = true,
            code = """
                id: bedrock_list_models
                namespace: company.team

                tasks:
                  - id: list_models
                    type: io.kestra.plugin.aws.bedrock.ListFoundationModels
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    byOutputModality: TEXT

                  - id: log_models
                    type: io.kestra.plugin.core.log.Log
                    message: "Available text models: {{ outputs.list_models.models }}"
                """
        )
    }
)
public class ListFoundationModels extends AbstractConnection implements RunnableTask<ListFoundationModels.Output> {

    @Schema(
        title = "Filter by provider",
        description = "Optional provider name to filter results, e.g. `amazon`, `anthropic`, `meta`, `mistral`."
    )
    @PluginProperty(group = "processing")
    private Property<String> byProvider;

    @Schema(
        title = "Filter by output modality",
        description = """
            Optional output modality filter. Valid values: `TEXT`, `IMAGE`, `EMBEDDING`.
            Any value not in this set will be rejected by the Bedrock API.
            """
    )
    @PluginProperty(group = "processing")
    private Property<String> byOutputModality;

    @Schema(
        title = "Filter by inference type",
        description = """
            Optional inference type filter. Valid values: `ON_DEMAND`, `PROVISIONED`.
            Any value not in this set will be rejected by the Bedrock API.
            """
    )
    @PluginProperty(group = "processing")
    private Property<String> byInferenceType;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var requestBuilder = ListFoundationModelsRequest.builder();

        if (this.byProvider != null) {
            requestBuilder.byProvider(runContext.render(this.byProvider).as(String.class).orElse(null));
        }
        if (this.byOutputModality != null) {
            var modality = runContext.render(this.byOutputModality).as(String.class).orElse(null);
            if (modality != null && ModelModality.fromValue(modality) == ModelModality.UNKNOWN_TO_SDK_VERSION) {
                throw new IllegalArgumentException(
                    "byOutputModality '" + modality + "' is not valid. Valid values: TEXT, IMAGE, EMBEDDING.");
            }
            requestBuilder.byOutputModality(modality);
        }
        if (this.byInferenceType != null) {
            var inferenceType = runContext.render(this.byInferenceType).as(String.class).orElse(null);
            if (inferenceType != null && InferenceType.fromValue(inferenceType) == InferenceType.UNKNOWN_TO_SDK_VERSION) {
                throw new IllegalArgumentException(
                    "byInferenceType '" + inferenceType + "' is not valid. Valid values: ON_DEMAND, PROVISIONED.");
            }
            requestBuilder.byInferenceType(inferenceType);
        }

        logger.debug("Listing Bedrock foundation models");

        try (var client = client(runContext)) {
            var response = client.listFoundationModels(requestBuilder.build());
            var models = response.modelSummaries().stream()
                .map(ListFoundationModels::summaryToMap)
                .toList();

            logger.debug("Found {} foundation models", models.size());
            return Output.builder().models(models).build();
        }
    }

    private static Map<String, Object> summaryToMap(FoundationModelSummary m) {
        var map = new HashMap<String, Object>();
        map.put("modelId", m.modelId() != null ? m.modelId() : "");
        map.put("modelName", m.modelName() != null ? m.modelName() : "");
        map.put("providerName", m.providerName() != null ? m.providerName() : "");
        map.put("outputModalities", m.outputModalities() != null
            ? m.outputModalities().stream().map(Object::toString).toList() : List.of());
        map.put("inputModalities", m.inputModalities() != null
            ? m.inputModalities().stream().map(Object::toString).toList() : List.of());
        map.put("inferenceTypesSupported", m.inferenceTypesSupported() != null
            ? m.inferenceTypesSupported().stream().map(Object::toString).toList() : List.of());
        map.put("customizationsSupported", m.customizationsSupported() != null
            ? m.customizationsSupported().stream().map(Object::toString).toList() : List.of());
        map.put("responseStreamingSupported", m.responseStreamingSupported());
        return map;
    }

    @VisibleForTesting
    BedrockClient client(RunContext runContext) throws Exception {
        var clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, BedrockClient.builder()).build();
    }

    @SuperBuilder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Foundation models",
            description = """
                List of available foundation models. Each entry contains `modelId`, `modelName`,
                `providerName`, `outputModalities`, `inputModalities`, `inferenceTypesSupported`,
                `customizationsSupported`, and `responseStreamingSupported`.
                """
        )
        private final List<Map<String, Object>> models;
    }
}

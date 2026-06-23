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
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.bedrock.BedrockClient;
import software.amazon.awssdk.services.bedrock.model.FoundationModelSummary;
import software.amazon.awssdk.services.bedrock.model.ListFoundationModelsRequest;
import software.amazon.awssdk.services.bedrock.model.ListFoundationModelsResponse;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List available Amazon Bedrock foundation models",
    description = "Returns the foundation models available in the configured AWS region, optionally filtered by provider, output modality, or inference type."
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
    private Property<String> byProvider;

    @Schema(
        title = "Filter by output modality",
        description = "Optional output modality filter. Common values: `TEXT`, `IMAGE`, `EMBEDDING`."
    )
    private Property<String> byOutputModality;

    @Schema(
        title = "Filter by inference type",
        description = "Optional inference type filter. Common values: `ON_DEMAND`, `PROVISIONED`."
    )
    private Property<String> byInferenceType;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        ListFoundationModelsRequest.Builder requestBuilder = ListFoundationModelsRequest.builder();

        if (this.byProvider != null) {
            requestBuilder.byProvider(runContext.render(this.byProvider).as(String.class).orElse(null));
        }
        if (this.byOutputModality != null) {
            requestBuilder.byOutputModality(runContext.render(this.byOutputModality).as(String.class).orElse(null));
        }
        if (this.byInferenceType != null) {
            requestBuilder.byInferenceType(runContext.render(this.byInferenceType).as(String.class).orElse(null));
        }

        logger.debug("Listing Bedrock foundation models");

        try (BedrockClient client = client(runContext)) {
            ListFoundationModelsResponse response = client.listFoundationModels(requestBuilder.build());

            List<Map<String, Object>> models = response.modelSummaries().stream()
                .map(ListFoundationModels::summaryToMap)
                .collect(Collectors.toList());

            logger.debug("Found {} foundation models", models.size());

            return Output.builder()
                .models(models)
                .build();
        }
    }

    private static Map<String, Object> summaryToMap(FoundationModelSummary m) {
        return Map.of(
            "modelId", m.modelId() != null ? m.modelId() : "",
            "modelName", m.modelName() != null ? m.modelName() : "",
            "providerName", m.providerName() != null ? m.providerName() : "",
            "outputModalities", m.outputModalities() != null
                ? m.outputModalities().stream().map(Object::toString).collect(Collectors.toList())
                : List.of(),
            "inferenceTypesSupported", m.inferenceTypesSupported() != null
                ? m.inferenceTypesSupported().stream().map(Object::toString).collect(Collectors.toList())
                : List.of()
        );
    }

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
            description = "List of available foundation models. Each entry contains `modelId`, `modelName`, `providerName`, `outputModalities`, and `inferenceTypesSupported`."
        )
        private final List<Map<String, Object>> models;
    }
}

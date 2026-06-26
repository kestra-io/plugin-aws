package io.kestra.plugin.aws.healthlake;

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
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.healthlake.HealthLakeClient;
import software.amazon.awssdk.services.healthlake.model.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Describe an Amazon HealthLake FHIR data store",
    description = """
        Returns the current status, ARN, endpoint, and creation time of an existing HealthLake FHIR data store.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Describe a HealthLake data store by ID.",
            full = true,
            code = """
                id: healthlake_describe_datastore
                namespace: company.team

                tasks:
                  - id: describe_store
                    type: io.kestra.plugin.aws.healthlake.DescribeDatastore
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    datastoreId: "{{ secret('HEALTHLAKE_DATASTORE_ID') }}"

                  - id: log_status
                    type: io.kestra.plugin.core.log.Log
                    message: "Datastore status: {{ outputs.describe_store.datastoreStatus }}"
                """
        )
    }
)
public class DescribeDatastore extends AbstractConnection implements RunnableTask<DescribeDatastore.Output> {

    @Schema(title = "Data store ID", description = "The ID of the FHIR data store to describe.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> datastoreId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var resolvedId = runContext.render(datastoreId).as(String.class).orElseThrow();

        var request = DescribeFHIRDatastoreRequest.builder()
            .datastoreId(resolvedId)
            .build();

        logger.debug("Describing HealthLake datastore '{}'", resolvedId);

        try (var client = client(runContext)) {
            var props = client.describeFHIRDatastore(request).datastoreProperties();
            return Output.builder()
                .datastoreId(props.datastoreId())
                .datastoreArn(props.datastoreArn())
                .datastoreName(props.datastoreName())
                .datastoreStatus(props.datastoreStatus().toString())
                .datastoreEndpoint(props.datastoreEndpoint())
                .createdAt(props.createdAt() != null ? props.createdAt().toString() : null)
                .build();
        }
    }

    @VisibleForTesting
    HealthLakeClient client(RunContext runContext) throws Exception {
        var clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, HealthLakeClient.builder()).build();
    }

    @SuperBuilder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Data store ID")
        private final String datastoreId;

        @Schema(title = "Data store ARN")
        private final String datastoreArn;

        @Schema(title = "Data store name")
        private final String datastoreName;

        @Schema(title = "Data store status", description = "One of: `CREATING`, `ACTIVE`, `DELETING`, `DELETED`, `CREATE_FAILED`.")
        private final String datastoreStatus;

        @Schema(title = "Data store endpoint", description = "The FHIR endpoint URL.")
        private final String datastoreEndpoint;

        @Schema(title = "Created at", description = "ISO-8601 creation timestamp.")
        private final String createdAt;
    }
}

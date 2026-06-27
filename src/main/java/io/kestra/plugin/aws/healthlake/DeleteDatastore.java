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
    title = "Delete an Amazon HealthLake FHIR data store",
    description = """
        Initiates deletion of a HealthLake FHIR data store and returns its final status.
        The data store must be in ACTIVE or CREATE_FAILED state to be deleted.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a HealthLake data store by ID.",
            full = true,
            code = """
                id: healthlake_delete_datastore
                namespace: company.team

                tasks:
                  - id: delete_store
                    type: io.kestra.plugin.aws.healthlake.DeleteDatastore
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    datastoreId: "{{ secret('HEALTHLAKE_DATASTORE_ID') }}"

                  - id: log_status
                    type: io.kestra.plugin.core.log.Log
                    message: "Datastore {{ outputs.delete_store.datastoreId }} status={{ outputs.delete_store.datastoreStatus }}"
                """
        )
    }
)
public class DeleteDatastore extends AbstractConnection implements RunnableTask<DeleteDatastore.Output> {

    @Schema(title = "Data store ID", description = "The ID of the FHIR data store to delete.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> datastoreId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var resolvedId = runContext.render(datastoreId).as(String.class).orElseThrow();

        var request = DeleteFHIRDatastoreRequest.builder()
            .datastoreId(resolvedId)
            .build();

        logger.debug("Deleting HealthLake datastore '{}'", resolvedId);

        try (var client = client(runContext)) {
            var response = client.deleteFHIRDatastore(request);
            logger.debug("Datastore '{}' deletion initiated, status={}", resolvedId, response.datastoreStatus());
            return Output.builder()
                .datastoreId(response.datastoreId())
                .datastoreArn(response.datastoreArn())
                .datastoreStatus(response.datastoreStatus().toString())
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

        @Schema(title = "Data store ID", description = "The ID of the deleted data store.")
        private final String datastoreId;

        @Schema(title = "Data store ARN", description = "The Amazon Resource Name of the deleted data store.")
        private final String datastoreArn;

        @Schema(title = "Data store status", description = "Status after deletion request, typically `DELETING`.")
        private final String datastoreStatus;
    }
}

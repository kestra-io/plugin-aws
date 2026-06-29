package io.kestra.plugin.aws.healthlake;

import java.util.UUID;

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
    title = "Create an Amazon HealthLake FHIR data store",
    description = """
        Provisions a new FHIR R4 data store in Amazon HealthLake and returns its ID, ARN, status, and endpoint.
        The data store is ready for import/export jobs once status reaches ACTIVE.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Create a FHIR R4 data store.",
            full = true,
            code = """
                id: healthlake_create_datastore
                namespace: company.team

                tasks:
                  - id: create_store
                    type: io.kestra.plugin.aws.healthlake.CreateDatastore
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    datastoreName: "my-fhir-datastore"

                  - id: log_id
                    type: io.kestra.plugin.core.log.Log
                    message: "Created datastore {{ outputs.create_store.datastoreId }} status={{ outputs.create_store.datastoreStatus }}"
                """
        )
    }
)
public class CreateDatastore extends AbstractConnection implements RunnableTask<CreateDatastore.Output> {

    @Schema(title = "Data store name", description = "A user-friendly name for the FHIR data store.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> datastoreName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var rName = runContext.render(datastoreName).as(String.class).orElseThrow();

        var request = CreateFhirDatastoreRequest.builder()
            .datastoreName(rName)
            .datastoreTypeVersion(FHIRVersion.R4)
            .clientToken(UUID.randomUUID().toString())
            .build();

        logger.debug("Creating HealthLake datastore '{}'", rName);

        try (var client = client(runContext)) {
            var response = client.createFHIRDatastore(request);
            logger.debug("Datastore created: id={} status={}", response.datastoreId(), response.datastoreStatus());
            return Output.builder()
                .datastoreId(response.datastoreId())
                .datastoreArn(response.datastoreArn())
                .datastoreStatus(response.datastoreStatus().toString())
                .datastoreEndpoint(response.datastoreEndpoint())
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

        @Schema(title = "Data store ID", description = "The generated ID of the new data store.")
        private final String datastoreId;

        @Schema(title = "Data store ARN", description = "The Amazon Resource Name of the data store.")
        private final String datastoreArn;

        @Schema(title = "Data store status", description = "Initial status of the data store, e.g. `CREATING`.")
        private final String datastoreStatus;

        @Schema(title = "Data store endpoint", description = "The FHIR endpoint URL of the data store.")
        private final String datastoreEndpoint;
    }
}

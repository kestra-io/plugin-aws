package io.kestra.plugin.aws.healthlake;

import java.util.ArrayList;
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
    title = "List Amazon HealthLake FHIR data stores",
    description = """
        Returns FHIR data stores in the configured AWS region with optional filtering by name or status.
        Paginates automatically. Results are capped at 100 entries per call; for larger accounts use the
        AWS Console or CLI for full enumeration.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "List all HealthLake data stores.",
            full = true,
            code = """
                id: healthlake_list_datastores
                namespace: company.team

                tasks:
                  - id: list_stores
                    type: io.kestra.plugin.aws.healthlake.ListDatastores
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"

                  - id: log_count
                    type: io.kestra.plugin.core.log.Log
                    message: "Found {{ outputs.list_stores.total }} HealthLake data stores"
                """
        )
    }
)
public class ListDatastores extends AbstractConnection implements RunnableTask<ListDatastores.Output> {

    private static final int MAX_RESULTS = 100;

    @Schema(
        title = "Filter by name",
        description = "Optional data store name substring filter."
    )
    @PluginProperty(group = "processing")
    private Property<String> filterName;

    @Schema(
        title = "Filter by status",
        description = "Optional status filter. Valid values: `CREATING`, `ACTIVE`, `DELETING`, `DELETED`, `CREATE_FAILED`."
    )
    @PluginProperty(group = "processing")
    private Property<String> filterStatus;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var filterBuilder = DatastoreFilter.builder();
        if (filterName != null) {
            filterBuilder.datastoreName(runContext.render(filterName).as(String.class).orElse(null));
        }
        if (filterStatus != null) {
            var statusStr = runContext.render(filterStatus).as(String.class).orElse(null);
            if (statusStr != null) {
                var status = DatastoreStatus.fromValue(statusStr);
                if (status == DatastoreStatus.UNKNOWN_TO_SDK_VERSION) {
                    throw new IllegalArgumentException("Unknown DatastoreStatus: " + statusStr);
                }
                filterBuilder.datastoreStatus(status);
            }
        }

        var datastores = new ArrayList<Map<String, Object>>();
        String nextToken = null;

        logger.debug("Listing HealthLake datastores");

        try (var client = client(runContext)) {
            do {
                var reqBuilder = ListFhirDatastoresRequest.builder().filter(filterBuilder.build());
                if (nextToken != null) {
                    reqBuilder.nextToken(nextToken);
                }
                var response = client.listFHIRDatastores(reqBuilder.build());
                for (var props : response.datastorePropertiesList()) {
                    datastores.add(
                        Map.of(
                            "datastoreId", props.datastoreId() != null ? props.datastoreId() : "",
                            "datastoreArn", props.datastoreArn() != null ? props.datastoreArn() : "",
                            "datastoreName", props.datastoreName() != null ? props.datastoreName() : "",
                            "datastoreStatus", props.datastoreStatus().toString(),
                            "datastoreEndpoint", props.datastoreEndpoint() != null ? props.datastoreEndpoint() : "",
                            "createdAt", props.createdAt() != null ? props.createdAt().toString() : ""
                        )
                    );
                    if (datastores.size() >= MAX_RESULTS) {
                        logger.warn("Result cap of {} reached; further data stores are omitted.", MAX_RESULTS);
                        nextToken = null;
                        break;
                    }
                }
                nextToken = datastores.size() < MAX_RESULTS ? response.nextToken() : null;
            } while (nextToken != null);
        }

        logger.debug("Found {} datastores", datastores.size());
        return Output.builder()
            .datastores(datastores)
            .total(datastores.size())
            .build();
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

        @Schema(
            title = "Data stores",
            description = "List of data store entries (capped at 100), each containing `datastoreId`, `datastoreArn`, `datastoreName`, `datastoreStatus`, `datastoreEndpoint`, and `createdAt`."
        )
        private final List<Map<String, Object>> datastores;

        @Schema(title = "Total", description = "Number of data stores returned (max 100).")
        private final Integer total;
    }
}

package io.kestra.plugin.aws.msk;

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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.DeleteClusterRequest;
import software.amazon.awssdk.services.kafka.model.DescribeClusterRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete an Amazon MSK cluster",
    description = """
        Initiates deletion of an MSK cluster and returns the cluster ARN and resulting state.
        Deletion is asynchronous — the cluster enters `DELETING` state immediately.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Delete an MSK cluster by ARN.",
            full = true,
            code = """
                id: msk_delete_cluster
                namespace: company.team

                tasks:
                  - id: delete_cluster
                    type: io.kestra.plugin.aws.msk.DeleteCluster
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    clusterArn: "{{ secret('MSK_CLUSTER_ARN') }}"

                  - id: log_state
                    type: io.kestra.plugin.core.log.Log
                    message: "Cluster state: {{ outputs.delete_cluster.state }}"
                """
        )
    }
)
public class DeleteCluster extends AbstractConnection implements RunnableTask<DeleteCluster.Output> {

    @Schema(title = "Cluster ARN", description = "The Amazon Resource Name of the MSK cluster to delete.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> clusterArn;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var resolvedArn = runContext.render(clusterArn).as(String.class).orElseThrow();

        logger.debug("Deleting MSK cluster '{}'", resolvedArn);

        // Single client instance for both describe (to fetch currentVersion) and delete
        try (var client = client(runContext)) {
            var describeResponse = client.describeCluster(
                DescribeClusterRequest.builder().clusterArn(resolvedArn).build());
            var currentVersion = describeResponse.clusterInfo().currentVersion();

            var response = client.deleteCluster(DeleteClusterRequest.builder()
                .clusterArn(resolvedArn)
                .currentVersion(currentVersion)
                .build());

            logger.debug("MSK cluster '{}' deletion initiated, state={}", resolvedArn, response.state());
            return Output.builder()
                .clusterArn(response.clusterArn())
                .state(response.state() != null ? response.state().toString() : null)
                .build();
        }
    }

    @VisibleForTesting
    KafkaClient client(RunContext runContext) throws Exception {
        var clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, KafkaClient.builder()).build();
    }

    @SuperBuilder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Cluster ARN", description = "The Amazon Resource Name of the deleted cluster.")
        private final String clusterArn;

        @Schema(title = "State", description = "State after deletion request, typically `DELETING`.")
        private final String state;
    }
}

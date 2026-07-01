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
import software.amazon.awssdk.services.kafka.model.DescribeClusterRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Describe an Amazon MSK cluster",
    description = """
        Returns the current state, broker count, Kafka version, ARN, and Zookeeper connection string
        for an MSK cluster. Use this after `CreateCluster` to poll for `ACTIVE` status.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Describe an MSK cluster by ARN.",
            full = true,
            code = """
                id: msk_describe_cluster
                namespace: company.team

                tasks:
                  - id: describe_cluster
                    type: io.kestra.plugin.aws.msk.DescribeCluster
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    clusterArn: "{{ secret('MSK_CLUSTER_ARN') }}"

                  - id: log_state
                    type: io.kestra.plugin.core.log.Log
                    message: "Cluster state: {{ outputs.describe_cluster.state }}"
                """
        )
    }
)
public class DescribeCluster extends AbstractConnection implements RunnableTask<DescribeCluster.Output> {

    @Schema(title = "Cluster ARN", description = "The Amazon Resource Name of the MSK cluster.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> clusterArn;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var resolvedArn = runContext.render(clusterArn).as(String.class).orElseThrow();

        var request = DescribeClusterRequest.builder()
            .clusterArn(resolvedArn)
            .build();

        logger.debug("Describing MSK cluster '{}'", resolvedArn);

        try (var client = client(runContext)) {
            var info = client.describeCluster(request).clusterInfo();
            var kafkaVersion = info.currentBrokerSoftwareInfo() != null
                ? info.currentBrokerSoftwareInfo().kafkaVersion() : null;
            return Output.builder()
                .clusterArn(info.clusterArn())
                .clusterName(info.clusterName())
                .state(info.state() != null ? info.state().toString() : null)
                .currentVersion(info.currentVersion())
                .kafkaVersion(kafkaVersion)
                .numberOfBrokerNodes(info.numberOfBrokerNodes())
                .zookeeperConnectString(info.zookeeperConnectString())
                .creationTime(info.creationTime() != null ? info.creationTime().toString() : null)
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

        @Schema(title = "Cluster ARN", description = "The Amazon Resource Name of the cluster.")
        private final String clusterArn;

        @Schema(title = "Cluster name", description = "The name of the cluster.")
        private final String clusterName;

        @Schema(title = "State", description = "Current cluster state. One of: `ACTIVE`, `CREATING`, `DELETING`, `FAILED`, `HEALING`, `MAINTENANCE`, `REBOOTING_BROKER`, `UPDATING`.")
        private final String state;

        @Schema(title = "Current version", description = "The current version of the cluster used for updates.")
        private final String currentVersion;

        @Schema(title = "Kafka version", description = "The Apache Kafka version running on the cluster.")
        private final String kafkaVersion;

        @Schema(title = "Number of broker nodes", description = "The number of broker nodes in the cluster.")
        private final Integer numberOfBrokerNodes;

        @Schema(title = "Zookeeper connect string", description = "Zookeeper connection string (plaintext).")
        private final String zookeeperConnectString;

        @Schema(title = "Creation time", description = "ISO-8601 timestamp when the cluster was created.")
        private final String creationTime;
    }
}

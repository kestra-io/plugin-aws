package io.kestra.plugin.aws.msk;

import java.util.List;

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
import software.amazon.awssdk.services.kafka.model.BrokerNodeGroupInfo;
import software.amazon.awssdk.services.kafka.model.CreateClusterRequest;
import software.amazon.awssdk.services.kafka.model.EBSStorageInfo;
import software.amazon.awssdk.services.kafka.model.StorageInfo;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create an Amazon MSK cluster",
    description = """
        Provisions a new MSK cluster and returns its ARN and initial state.
        Cluster creation is asynchronous — use `DescribeCluster` or the `Trigger` task to
        poll for `ACTIVE` state before connecting Kafka producers or consumers.
        Note: this task creates a cluster with default encryption settings. For production
        workloads requiring encryption-in-transit or at-rest, configure those settings via
        the AWS Console or CLI after cluster creation.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Create an MSK cluster and retrieve bootstrap brokers.",
            full = true,
            code = """
                id: provision_msk_cluster
                namespace: company.team

                tasks:
                  - id: create_cluster
                    type: io.kestra.plugin.aws.msk.CreateCluster
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    clusterName: "my-kestra-msk"
                    kafkaVersion: "3.5.1"
                    numberOfBrokerNodes: 3
                    instanceType: "kafka.m5.large"
                    clientSubnets:
                      - "{{ secret('SUBNET_A') }}"
                      - "{{ secret('SUBNET_B') }}"
                      - "{{ secret('SUBNET_C') }}"
                    securityGroups:
                      - "{{ secret('SECURITY_GROUP_ID') }}"

                  - id: log_arn
                    type: io.kestra.plugin.core.log.Log
                    message: "MSK cluster ARN: {{ outputs.create_cluster.clusterArn }}"
                """
        )
    }
)
public class CreateCluster extends AbstractConnection implements RunnableTask<CreateCluster.Output> {

    @Schema(title = "Cluster name", description = "A unique name for the MSK cluster.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> clusterName;

    @Schema(title = "Kafka version", description = "The Apache Kafka version for the cluster, e.g. `3.5.1`.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> kafkaVersion;

    @Schema(title = "Number of broker nodes", description = "Total number of broker nodes. Must be a multiple of the number of Availability Zones.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<Integer> numberOfBrokerNodes;

    @Schema(title = "Instance type", description = "Broker instance type, e.g. `kafka.m5.large`.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> instanceType;

    @Schema(title = "Client subnets", description = "List of subnet IDs for broker node placement — one per Availability Zone.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<List<String>> clientSubnets;

    @Schema(title = "Security groups", description = "List of security group IDs to associate with the broker nodes.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<List<String>> securityGroups;

    @Schema(title = "EBS volume size (GiB)", description = "Storage volume size per broker in GiB. Defaults to 100.")
    @PluginProperty(group = "processing")
    private Property<Integer> ebsVolumeSize;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rName = runContext.render(clusterName).as(String.class).orElseThrow();
        var rKafkaVersion = runContext.render(kafkaVersion).as(String.class).orElseThrow();
        var rBrokerCount = runContext.render(numberOfBrokerNodes).as(Integer.class).orElseThrow();
        var rInstanceType = runContext.render(instanceType).as(String.class).orElseThrow();
        var rSubnets = runContext.render(clientSubnets).asList(String.class);
        var rSecurityGroups = runContext.render(securityGroups).asList(String.class);
        var rVolumeSize = ebsVolumeSize != null
            ? runContext.render(ebsVolumeSize).as(Integer.class).orElse(100)
            : 100;

        var request = CreateClusterRequest.builder()
            .clusterName(rName)
            .kafkaVersion(rKafkaVersion)
            .numberOfBrokerNodes(rBrokerCount)
            .brokerNodeGroupInfo(BrokerNodeGroupInfo.builder()
                .instanceType(rInstanceType)
                .clientSubnets(rSubnets)
                .securityGroups(rSecurityGroups)
                .storageInfo(StorageInfo.builder()
                    .ebsStorageInfo(EBSStorageInfo.builder()
                        .volumeSize(rVolumeSize)
                        .build())
                    .build())
                .build())
            .build();

        logger.debug("Creating MSK cluster '{}' with Kafka {} and {} brokers", rName, rKafkaVersion, rBrokerCount);

        try (var client = client(runContext)) {
            var response = client.createCluster(request);
            logger.debug("MSK cluster creation initiated: arn={} state={}", response.clusterArn(), response.state());
            return Output.builder()
                .clusterArn(response.clusterArn())
                .clusterName(response.clusterName())
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

        @Schema(title = "Cluster ARN", description = "The Amazon Resource Name of the newly created cluster.")
        private final String clusterArn;

        @Schema(title = "Cluster name", description = "The name of the cluster.")
        private final String clusterName;

        @Schema(title = "State", description = "Initial state of the cluster, typically `CREATING`.")
        private final String state;
    }
}

package io.kestra.plugin.aws.msk;

import java.time.Duration;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.DescribeClusterRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow when an MSK cluster reaches a target state",
    description = """
        Polls the state of an MSK cluster at a fixed interval and fires when the cluster
        reaches the configured `targetState`. Exposes `{{ trigger.clusterArn }}` and
        `{{ trigger.clusterState }}` to downstream tasks.
        Note: the minimum recommended poll interval is PT30S.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a flow when an MSK cluster becomes ACTIVE.",
            full = true,
            code = """
                id: on_msk_cluster_active
                namespace: company.team

                triggers:
                  - id: cluster_ready
                    type: io.kestra.plugin.aws.msk.Trigger
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    clusterArn: "{{ secret('MSK_CLUSTER_ARN') }}"
                    targetState: ACTIVE
                    interval: PT1M

                tasks:
                  - id: notify
                    type: io.kestra.plugin.core.log.Log
                    message: "MSK cluster {{ trigger.clusterArn }} is now {{ trigger.clusterState }}"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, AbstractConnectionInterface {

    // Connection fields — groups aligned with kinesis.Trigger and AbstractConnectionInterface
    @Schema(title = "AWS access key ID", description = "Optional static credential. If omitted, the default credentials provider chain is used.")
    @PluginProperty(secret = true, group = "advanced")
    protected Property<String> accessKeyId;

    @Schema(title = "AWS secret access key", description = "Pairs with `accessKeyId` for static credentials.")
    @PluginProperty(secret = true, group = "advanced")
    protected Property<String> secretKeyId;

    @Schema(title = "AWS session token", description = "Session token for temporary credentials.")
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> sessionToken;

    @Schema(title = "AWS region", description = "The AWS region for the MSK service.")
    @PluginProperty(group = "connection")
    protected Property<String> region;

    @Schema(title = "Endpoint override", description = "Override the default AWS endpoint URL.")
    @PluginProperty(group = "advanced")
    protected Property<String> endpointOverride;

    @Schema(title = "STS role ARN", description = "IAM role ARN to assume via STS before making API calls.")
    @PluginProperty(secret = true, group = "advanced")
    protected Property<String> stsRoleArn;

    @Schema(title = "STS role external ID", description = "External ID to pass when assuming the STS role.")
    @PluginProperty(secret = true, group = "advanced")
    protected Property<String> stsRoleExternalId;

    @Schema(title = "STS role session name", description = "Session name to use when assuming the STS role.")
    @PluginProperty(group = "advanced")
    protected Property<String> stsRoleSessionName;

    @Schema(title = "STS endpoint override", description = "Override the default STS endpoint URL.")
    @PluginProperty(group = "advanced")
    protected Property<String> stsEndpointOverride;

    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    @Schema(title = "Poll interval", description = "How often to poll the cluster state. ISO-8601 duration, e.g. `PT1M`. Minimum recommended value: `PT30S`.")
    @PluginProperty(group = "main")
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "Cluster ARN", description = "The Amazon Resource Name of the MSK cluster to monitor.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> clusterArn;

    @Schema(
        title = "Target state",
        description = "The cluster state that triggers execution. Valid values: `ACTIVE`, `CREATING`, `DELETING`, `FAILED`, `HEALING`, `MAINTENANCE`, `REBOOTING_BROKER`, `UPDATING`."
    )
    @PluginProperty(group = "main")
    @NotNull
    private Property<ClusterState> targetState;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        var rArn = runContext.render(clusterArn).as(String.class).orElseThrow();
        var rTargetState = runContext.render(targetState).as(ClusterState.class).orElseThrow();

        try (var client = client(runContext)) {
            var info = client.describeCluster(
                DescribeClusterRequest.builder().clusterArn(rArn).build()
            ).clusterInfo();

            var currentState = info.state();
            logger.debug("MSK cluster '{}' current state={}, waiting for {}", rArn, currentState, rTargetState);

            if (currentState != rTargetState) {
                return Optional.empty();
            }

            logger.debug("MSK cluster '{}' reached target state={}, firing trigger", rArn, rTargetState);
            var output = Output.builder()
                .clusterArn(rArn)
                .clusterState(currentState.toString())
                .build();
            return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
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

        @Schema(title = "Cluster ARN", description = "The ARN of the cluster that reached the target state.")
        private final String clusterArn;

        @Schema(title = "Cluster state", description = "The state the cluster reached, e.g. `ACTIVE`.")
        private final String clusterState;
    }
}

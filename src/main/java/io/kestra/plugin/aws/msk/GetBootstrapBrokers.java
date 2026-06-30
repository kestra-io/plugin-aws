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
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get bootstrap broker strings for an Amazon MSK cluster",
    description = """
        Returns all available bootstrap broker connection strings for an MSK cluster.
        Use the returned broker strings to configure `plugin-kafka` tasks that produce or consume
        messages from the cluster.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Get bootstrap brokers for an MSK cluster.",
            full = true,
            code = """
                id: msk_get_brokers
                namespace: company.team

                tasks:
                  - id: get_brokers
                    type: io.kestra.plugin.aws.msk.GetBootstrapBrokers
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    clusterArn: "{{ secret('MSK_CLUSTER_ARN') }}"

                  - id: log_brokers
                    type: io.kestra.plugin.core.log.Log
                    message: "Bootstrap brokers: {{ outputs.get_brokers.bootstrapBrokerString }}"
                """
        )
    }
)
public class GetBootstrapBrokers extends AbstractConnection implements RunnableTask<GetBootstrapBrokers.Output> {

    @Schema(title = "Cluster ARN", description = "The Amazon Resource Name of the MSK cluster.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> clusterArn;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var resolvedArn = runContext.render(clusterArn).as(String.class).orElseThrow();

        var request = GetBootstrapBrokersRequest.builder()
            .clusterArn(resolvedArn)
            .build();

        logger.debug("Getting bootstrap brokers for MSK cluster '{}'", resolvedArn);

        try (var client = client(runContext)) {
            var response = client.getBootstrapBrokers(request);
            return Output.builder()
                .bootstrapBrokerString(response.bootstrapBrokerString())
                .bootstrapBrokerStringTls(response.bootstrapBrokerStringTls())
                .bootstrapBrokerStringSaslScram(response.bootstrapBrokerStringSaslScram())
                .bootstrapBrokerStringSaslIam(response.bootstrapBrokerStringSaslIam())
                .bootstrapBrokerStringPublicTls(response.bootstrapBrokerStringPublicTls())
                .bootstrapBrokerStringPublicSaslScram(response.bootstrapBrokerStringPublicSaslScram())
                .bootstrapBrokerStringPublicSaslIam(response.bootstrapBrokerStringPublicSaslIam())
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

        @Schema(title = "Bootstrap broker string", description = "Plaintext broker connection string for use with non-TLS clients.")
        private final String bootstrapBrokerString;

        @Schema(title = "Bootstrap broker string (TLS)", description = "TLS-encrypted broker connection string.")
        private final String bootstrapBrokerStringTls;

        @Schema(title = "Bootstrap broker string (SASL/SCRAM)", description = "SASL/SCRAM broker connection string.")
        private final String bootstrapBrokerStringSaslScram;

        @Schema(title = "Bootstrap broker string (SASL/IAM)", description = "SASL/IAM broker connection string for IAM-based authentication.")
        private final String bootstrapBrokerStringSaslIam;

        @Schema(title = "Bootstrap broker string (public TLS)", description = "Public TLS broker connection string for cross-VPC access.")
        private final String bootstrapBrokerStringPublicTls;

        @Schema(title = "Bootstrap broker string (public SASL/SCRAM)", description = "Public SASL/SCRAM broker connection string.")
        private final String bootstrapBrokerStringPublicSaslScram;

        @Schema(title = "Bootstrap broker string (public SASL/IAM)", description = "Public SASL/IAM broker connection string.")
        private final String bootstrapBrokerStringPublicSaslIam;
    }
}

package io.kestra.plugin.aws.glue;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.glue.GlueClient;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Shared Glue connection",
    description = "Provides a GlueClient using standard AWS connection settings."
)
public abstract class AbstractGlueTask extends AbstractConnection {
    protected GlueClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, GlueClient.builder()).build();
    }

    protected GlueClient glueClient(RunContext runContext) throws IllegalVariableEvaluationException {
        return client(runContext);
    }
}

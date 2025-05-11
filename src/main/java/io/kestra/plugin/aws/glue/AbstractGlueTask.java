package io.kestra.plugin.aws.glue;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
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
public abstract class AbstractGlueTask extends AbstractConnection {
    protected GlueClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, GlueClient.builder()).build();
    }
}
package io.kestra.plugin.aws.cloudformation;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCloudFormation extends AbstractConnection {

    @NotNull
    @Schema(
        title = "Stack name",
        description = "CloudFormation stack identifier used by create, update, and delete operations."
    )
    protected Property<String> stackName;

    @Builder.Default
    @Schema(
        title = "Wait for completion",
        description = "When true (default), block until the stack reaches a terminal state for the requested operation."
    )
    protected Property<Boolean> waitForCompletion = Property.of(true);

    protected CloudFormationClient cfClient(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, CloudFormationClient.builder()).build();
    }
}

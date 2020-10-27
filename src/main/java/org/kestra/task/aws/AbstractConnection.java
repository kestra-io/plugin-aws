package org.kestra.task.aws;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractConnection extends Task {
    @Schema(
        title = "The Access Key Id in order to connect to AWS",
        description = "If no connection is defined, we will use default DefaultCredentialsProvider that will try to guess the value"
    )
    @PluginProperty(dynamic = true)
    private String accessKeyId;

    @Schema(
        title = "The Secret Key Id in order to connect to AWS",
        description = "If no connection is defined, we will use default DefaultCredentialsProvider that will try to guess the value"
    )
    @PluginProperty(dynamic = true)
    private String secretKeyId;

    protected AwsCredentialsProvider credentials(RunContext runContext) throws IllegalVariableEvaluationException {
        String accessKeyId = runContext.render(this.accessKeyId);
        String secretKeyId = runContext.render(this.secretKeyId);

        if (accessKeyId != null && secretKeyId != null) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(
                accessKeyId,
                secretKeyId
            ));
        }

        return DefaultCredentialsProvider.builder()
            .build();
    }
}

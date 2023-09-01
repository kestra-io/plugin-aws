package io.kestra.plugin.aws;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.auth.credentials.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractConnection extends Task implements AbstractConnectionInterface {
    protected String accessKeyId;

    protected String secretKeyId;

    protected String sessionToken;

    protected String region;

    protected String endpointOverride;

    private Boolean useDefaultAsyncClient;

    protected AwsCredentialsProvider credentials(RunContext runContext) throws IllegalVariableEvaluationException {
        String accessKeyId = runContext.render(this.accessKeyId);
        String secretKeyId = runContext.render(this.secretKeyId);
        String sessionToken = runContext.render(this.sessionToken);

        if (sessionToken != null) {
            StaticCredentialsProvider.create(AwsSessionCredentials.create(
                accessKeyId,
                secretKeyId,
                sessionToken
            ));
        } else if (accessKeyId != null && secretKeyId != null) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(
                accessKeyId,
                secretKeyId
            ));
        }

        return DefaultCredentialsProvider.builder()
            .build();
    }
}

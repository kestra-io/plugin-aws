package org.kestra.task.aws;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractConnection extends Task {
    @InputProperty(
        description = "The Access Key Id in order to connect to AWS",
        dynamic = true,
        body = {
            "If no connection is defined, we will use default DefaultCredentialsProvider that will try to guess the value",
        }
    )
    private String accessKeyId;

    @InputProperty(
        description = "The Secret Key Id in order to connect to AWS",
        dynamic = true,
        body = {
            "If no connection is defined, we will use default DefaultCredentialsProvider that will try to guess the value",
        }
    )
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

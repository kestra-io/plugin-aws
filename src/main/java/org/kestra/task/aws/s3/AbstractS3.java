package org.kestra.task.aws.s3;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.task.aws.AbstractConnection;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractS3 extends AbstractConnection {
    @InputProperty(
        description = "The AWS region to used",
        dynamic = true
    )
    private String region;

    S3Client client(RunContext runContext) throws IllegalVariableEvaluationException {
        S3ClientBuilder s3ClientBuilder = S3Client.builder()
            .credentialsProvider(this.credentials(runContext));

        String region = runContext.render(this.region);
        if (this.region != null) {
            s3ClientBuilder.region(Region.of(region));
        }

        return s3ClientBuilder.build();
    }
}

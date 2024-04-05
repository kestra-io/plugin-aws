package io.kestra.plugin.aws.runner;

import io.kestra.core.models.tasks.runners.AbstractTaskRunnerTest;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Disabled;

import java.time.Duration;

@MicronautTest
@Disabled("Too costly to run on CI")
public class AwsBatchTaskRunnerTest extends AbstractTaskRunnerTest {
    @Value("${kestra.aws.batch.accessKeyId}")
    private String accessKeyId;

    @Value("${kestra.aws.batch.secretKeyId}")
    private String secretKeyId;

    @Value("${kestra.aws.batch.s3Bucket}")
    private String s3Bucket;


    @Override
    protected TaskRunner taskRunner() {
        return AwsBatchTaskRunner.builder()
            .accessKeyId(accessKeyId)
            .secretKeyId(secretKeyId)
            .bucket(s3Bucket)
            .region("eu-west-3")
            .computeEnvironmentArn("arn:aws:batch:eu-west-3:634784741179:compute-environment/FargateComputeEnvironment")
            .executionRoleArn("arn:aws:iam::634784741179:role/AWS-Batch-Role-For-Fargate")
            .taskRoleArn("arn:aws:iam::634784741179:role/S3-Within-AWS-Batch")
            .waitUntilCompletion(Duration.ofMinutes(30))
            .jobQueueArn("arn:aws:batch:eu-west-3:634784741179:job-queue/queue")
            .build();
    }

    @Override
    protected boolean needsToSpecifyWorkingDirectory() {
        return true;
    }
}

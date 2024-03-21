package io.kestra.plugin.aws.runner;

import io.kestra.core.models.script.AbstractLogConsumer;
import io.kestra.core.models.script.RunnerResult;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.runners.DefaultLogConsumer;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

@MicronautTest
public class AwsBatchScriptRunnerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.aws.batch.accessKeyId}")
    private String accessKeyId;

    @Value("${kestra.aws.batch.secretKeyId}")
    private String secretKeyId;

    @Value("${kestra.aws.batch.s3Bucket}")
    private String s3Bucket;

    @Test
    @Disabled
    void run() throws Exception {
        AwsBatchScriptRunner runner = AwsBatchScriptRunner.builder()
            .accessKeyId(accessKeyId)
            .secretKeyId(secretKeyId)
            .s3Bucket(s3Bucket)
            .region("eu-west-3")
            .computeEnvironmentArn("arn:aws:batch:eu-west-3:634784741179:compute-environment/FargateComputeEnvironment")
            .executionRoleArn("arn:aws:iam::634784741179:role/AWS-Batch-Role-For-Fargate")
            .waitUntilCompletion(Duration.ofMinutes(30))
            .build();

        RunContext runContext = runContextFactory.of();

        Path workingDirectory = runContext.tempDir();
        File file = workingDirectory.resolve("hello.txt").toFile();
        FileUtils.writeStringToFile(file, "Hello World", "UTF-8");

        DefaultLogConsumer defaultLogConsumer = new DefaultLogConsumer(runContext);
        // This is purely to showcase that no logs is sent as STDERR for now as CloudWatch doesn't seem to send such information.
        Map<String, Boolean> logsWithIsStdErr = new HashMap<>();
        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withCommands(List.of(
                "/bin/bash", "-c",
                "s3 cp {{workingDir}}/hello.txt hello.txt",
                "cat hello.txt",
                "s3 cp hello.txt {{outputDir}}/output.txt"
            ))
            .withContainerImage("amazonlinux")
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String log, Boolean isStdErr) {
                    logsWithIsStdErr.put(log, isStdErr);
                    defaultLogConsumer.accept(log, isStdErr);
                }
            });
        RunnerResult run = runner.run(runContext, commandsWrapper, List.of("hello.txt"), List.of("output.txt"));

        MatcherAssert.assertThat(run.getExitCode(), is(0));
        MatcherAssert.assertThat(logsWithIsStdErr.get("[JOB LOG] Hello World"), is(false));
    }
}

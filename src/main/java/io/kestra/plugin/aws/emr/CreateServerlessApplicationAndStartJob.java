package io.kestra.plugin.aws.emr;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Create an EMR Serverless application and immediately start a job run.")
@Plugin(
    examples = {
        @Example(
            title = "Create an EMR Serverless app and start a Spark job",
            full = true,
            code = """
                id: create_and_run_emr_serverless
                namespace: company.team

                tasks:
                  - id: create_and_run
                    type: io.kestra.plugin.aws.emr.CreateServerlessApplicationAndStartJob
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    releaseLabel: "emr-7.0.0"
                    applicationType: "SPARK"
                    executionRoleArn: "arn:aws:iam::123456789012:role/EMRServerlessRole"
                    jobName: "example-job"
                    entryPoint: "s3://my-bucket/jobs/script.py"
                """
        )
    }
)
public class CreateServerlessApplicationAndStartJob extends AbstractEmrServerlessTask implements RunnableTask<CreateServerlessApplicationAndStartJob.Output> {

    @Schema(
        title = "The EMR release label to use for the application.",
        description = "For example, `emr-6.3.0` or `emr-7.0.0`."
    )
    @NotNull
    private Property<String> releaseLabel;

    @Schema(
        title = "The type of application to create.",
        description = "Valid values are for instance `SPARK` and `HIVE`."
    )
    @NotNull
    private Property<String> applicationType;

    @Schema(
        title = "The execution role ARN for the application.",
        description = "This role will be assumed by EMR Serverless to access AWS resources on your behalf."
    )
    @NotNull
    private Property<String> executionRoleArn;

    @Schema(
        title = "The name of the job to start."
    )
    @NotNull
    private Property<String> jobName;

    @Schema(
        title = "The entry point for the job.",
        description = "For `SPARK` applications, this is typically the S3 path to your main application file (e.g., a Python or JAR file). For `HIVE` applications, this is the Hive query to execute."
    )
    @NotNull
    private Property<String> entryPoint;

    @Override
    public Output run(RunContext runContext) throws IllegalVariableEvaluationException {
        try (EmrServerlessClient client = this.client(runContext)) {
            String rReleaseLabel = runContext.render(releaseLabel).as(String.class).orElseThrow();
            String rApplicationType = runContext.render(applicationType).as(String.class).orElseThrow();
            String rExecutionRoleArn = runContext.render(executionRoleArn).as(String.class).orElseThrow();
            String rJobName = runContext.render(jobName).as(String.class).orElseThrow();
            String rEntryPoint = runContext.render(entryPoint).as(String.class).orElseThrow();

            // 1. Create Application
            CreateApplicationResponse app = client.createApplication(CreateApplicationRequest.builder()
                .releaseLabel(rReleaseLabel)
                .type(rApplicationType)
                .build());

            // 2. Start Job
            StartJobRunRequest.Builder jobBuilder = StartJobRunRequest.builder()
                .applicationId(app.applicationId())
                .executionRoleArn(rExecutionRoleArn)
                .name(rJobName);

            if ("SPARK".equalsIgnoreCase(rApplicationType)) {
                jobBuilder.jobDriver(jd -> jd.sparkSubmit(ss -> ss.entryPoint(rEntryPoint)));
            } else if ("HIVE".equalsIgnoreCase(rApplicationType)) {
                jobBuilder.jobDriver(jd -> jd.hive(hd -> hd.query(rEntryPoint)));
            } else {
                throw new IllegalArgumentException("Unsupported application rApplicationType: " + rApplicationType);
            }

            StartJobRunResponse job = client.startJobRun(jobBuilder.build());

            runContext.logger().info("Created app {} and started job {}", app.applicationId(), job.jobRunId());

            return Output.builder()
                .applicationId(app.applicationId())
                .jobRunId(job.jobRunId())
                .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create EMR Serverless app and start job", e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private final String applicationId;
        private final String jobRunId;
    }
}

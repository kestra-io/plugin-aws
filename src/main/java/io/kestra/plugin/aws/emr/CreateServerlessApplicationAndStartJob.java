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
@Schema(
    title = "Create EMR Serverless app and start job",
    description = "Creates an EMR Serverless application then starts a job run immediately on it. Returns applicationId and jobRunId; does not wait for job completion. Supports SPARK or HIVE job drivers based on applicationType."
)
@Plugin(
    examples = {
        @Example(
            title = "Create an EMR Serverless app and start a Spark job",
            full = true,
            code = """
                id: create_and_run_emr_serverless_app
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
        title = "Release label",
        description = "EMR Serverless release, e.g., emr-6.3.0 or emr-7.0.0."
    )
    @NotNull
    private Property<String> releaseLabel;

    @Schema(
        title = "Application type",
        description = "Valid values SPARK or HIVE; determines which job driver is built."
    )
    @NotNull
    private Property<String> applicationType;

    @Schema(
        title = "Execution role ARN",
        description = "IAM role assumed by EMR Serverless for the application."
    )
    @NotNull
    private Property<String> executionRoleArn;

    @Schema(
        title = "Job name",
        description = "Name reported for the started job run."
    )
    @NotNull
    private Property<String> jobName;

    @Schema(
        title = "Job entry point",
        description = "For SPARK, S3 URI to the main file passed to spark-submit; for HIVE, the Hive query text."
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

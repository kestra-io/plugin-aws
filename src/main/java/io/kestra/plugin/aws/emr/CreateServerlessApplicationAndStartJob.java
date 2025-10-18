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
    @NotNull private Property<String> releaseLabel;
    @NotNull private Property<String> applicationType;
    @NotNull private Property<String> executionRoleArn;
    @NotNull private Property<String> jobName;
    @NotNull private Property<String> entryPoint;

    @Override
    public Output run(RunContext runContext) throws IllegalVariableEvaluationException {
        try (EmrServerlessClient client = this.client(runContext)) {
            String release = runContext.render(releaseLabel).as(String.class).orElseThrow();
            String type = runContext.render(applicationType).as(String.class).orElseThrow();
            String role = runContext.render(executionRoleArn).as(String.class).orElseThrow();
            String name = runContext.render(jobName).as(String.class).orElseThrow();
            String entry = runContext.render(entryPoint).as(String.class).orElseThrow();

            // 1. Create Application
            CreateApplicationResponse app = client.createApplication(CreateApplicationRequest.builder()
                .releaseLabel(release)
                .type(type)
                .build());

            // 2. Start Job
            StartJobRunRequest.Builder jobBuilder = StartJobRunRequest.builder()
                .applicationId(app.applicationId())
                .executionRoleArn(role)
                .name(name);

            if ("SPARK".equalsIgnoreCase(type)) {
                jobBuilder.jobDriver(jd -> jd.sparkSubmit(ss -> ss.entryPoint(entry)));
            } else if ("HIVE".equalsIgnoreCase(type)) {
                jobBuilder.jobDriver(jd -> jd.hive(hd -> hd.query(entry)));
            } else {
                throw new IllegalArgumentException("Unsupported application type: " + type);
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

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
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunResponse;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Start a job run on an existing EMR Serverless application.")
@Plugin(
    examples = {
        @Example(
            title = "Start EMR Serverless job",
            full = true,
            code = """
                id: start_emr_job
                namespace: company.team

                tasks:
                  - id: start_job
                    type: io.kestra.plugin.aws.emr.StartServerlessJobRun
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    applicationId: "00f123abc456xyz"
                    executionRoleArn: "arn:aws:iam::123456789012:role/EMRServerlessRole"
                    jobName: "sample-spark-job"
                    entryPoint: "s3://my-bucket/scripts/spark-app.py"
                    jobDriver:
                      sparkSubmit:
                        entryPointArguments:
                          - "--arg1"
                          - "value1"
                """
        )
    }
)
public class StartServerlessJobRun extends AbstractEmrServerlessTask implements RunnableTask<StartServerlessJobRun.Output> {

    @Schema(
        title = "The EMR Serverless application ID to run the job on."
    )
    @NotNull
    private Property<String> applicationId;

    @Schema(
        title = "The execution role ARN for the job."
    )
    @NotNull
    private Property<String> executionRoleArn;

    @Schema(
        title = "The name of the job."
    )
    @NotNull
    private Property<String> jobName;

    @Schema(
        title = "The entry point for the job."
    )
    @NotNull
    private Property<String> entryPoint;

    @Override
    public Output run(RunContext runContext) throws IllegalVariableEvaluationException {
        try (EmrServerlessClient client = this.client(runContext)) {
            String rApplicationId = runContext.render(applicationId).as(String.class).orElseThrow();
            String rExecutionRoleArn = runContext.render(executionRoleArn).as(String.class).orElseThrow();
            String rJobName = runContext.render(jobName).as(String.class).orElseThrow();
            String rEntryPoint = runContext.render(entryPoint).as(String.class).orElseThrow();

            StartJobRunRequest request = StartJobRunRequest.builder()
                .applicationId(rApplicationId)
                .executionRoleArn(rExecutionRoleArn)
                .name(rJobName)
                .jobDriver(builder -> builder.sparkSubmit(builder2 -> builder2.entryPoint(rEntryPoint)))
                .build();

            StartJobRunResponse response = client.startJobRun(request);

            runContext.logger().info("Started EMR Serverless job: {}", response.jobRunId());

            return Output.builder()
                .jobRunId(response.jobRunId())
                .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start EMR Serverless job", e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Job Run ID")
        private final String jobRunId;
    }
}
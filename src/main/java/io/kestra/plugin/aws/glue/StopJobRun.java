package io.kestra.plugin.aws.glue;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.plugin.aws.glue.model.Output;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.BatchStopJobRunRequest;
import software.amazon.awssdk.services.glue.model.BatchStopJobRunResponse;
import software.amazon.awssdk.services.glue.model.GetJobRunRequest;
import software.amazon.awssdk.services.glue.model.GetJobRunResponse;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.plugin.aws.glue.GlueService.createGetJobRunRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Stop a running AWS Glue job and optionally wait for completion."
)
@Plugin(
    examples = {
        @Example(
            title = "Stop a Glue job and wait for its completion",
            full = true,
            code = """
                id: stop_glue_job
                namespace: company.team

                tasks:
                  - id: stop
                    type: io.kestra.plugin.aws.glue.StopJobRun
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    jobName: my-glue-job
                    jobRunId: jr_1234567890abcdef
                    wait: true
                    interval: PT1S
                """
        )
    }
)
public class StopJobRun extends AbstractGlueTask implements RunnableTask<Output> {

    @Schema(title = "The name of the Glue job to stop")
    @PluginProperty(dynamic = true)
    @NotNull
    private Property<String> jobName;

    @Schema(title = "The ID of the job run to stop")
    @PluginProperty(dynamic = true)
    @NotNull
    private Property<String> jobRunId;

    @Schema(
        title = "Wait for the job to be fully stopped before ending the task.",
        description = "If true, the task will periodically check the job status until it reaches a stopped state."
    )
    @Builder.Default
    private Property<Boolean> wait = Property.ofValue(true);

    @Schema(
        title = "Interval between status checks"
    )
    @Builder.Default
    private Property<Duration> interval = Property.ofValue(Duration.ofSeconds(1));

    @Override
    public Output run(RunContext runContext) throws Exception {
        // Render properties once
        String jobNameValue = runContext.render(this.jobName).as(String.class).orElseThrow();
        String jobRunIdValue = runContext.render(this.jobRunId).as(String.class).orElseThrow();
        boolean waitValue = runContext.render(this.wait).as(Boolean.class).orElse(true);
        Duration intervalValue = runContext.render(this.interval).as(Duration.class).orElse(Duration.ofSeconds(1));

        try (GlueClient glueClient = this.client(runContext)) {
            // Stop the job
            BatchStopJobRunResponse stopResponse = glueClient.batchStopJobRun(
                BatchStopJobRunRequest.builder()
                    .jobName(jobNameValue)
                    .jobRunIds(jobRunIdValue)
                    .build()
            );

            runContext.logger().info("Stopped Glue job '{}' with run ID: {}", jobNameValue, jobRunIdValue);

            GetJobRunRequest getJobRunRequest = createGetJobRunRequest(jobNameValue, jobRunIdValue);
            AtomicReference<software.amazon.awssdk.services.glue.model.JobRun> currentJobRun = new AtomicReference<>();

            GetJobRunResponse initialResponse = glueClient.getJobRun(getJobRunRequest);
            software.amazon.awssdk.services.glue.model.JobRun initialJobRun = initialResponse.jobRun();
            currentJobRun.set(initialJobRun);

            runContext.logger().info("Initial job state after stop request: {}", initialJobRun.jobRunStateAsString());

            // Wait for job to reach terminal state
            if (waitValue) {
                waitForJobStopped(runContext, glueClient, getJobRunRequest, currentJobRun, intervalValue);
            }

            // Check state
            var acceptableStopStates = Set.of(
                software.amazon.awssdk.services.glue.model.JobRunState.STOPPED,
                software.amazon.awssdk.services.glue.model.JobRunState.SUCCEEDED,
                software.amazon.awssdk.services.glue.model.JobRunState.FAILED
            );

            var finalState = currentJobRun.get().jobRunState();
            if (!acceptableStopStates.contains(finalState)) {
                throw new RuntimeException("Job failed to stop. Final state: " + finalState +
                    (currentJobRun.get().errorMessage() != null ?
                        ", Error message: " + currentJobRun.get().errorMessage() : ""));
            }

            return buildOutput(jobNameValue, jobRunIdValue, currentJobRun.get());
        }
    }

    private void waitForJobStopped(RunContext runContext, GlueClient glueClient,
                                   GetJobRunRequest getJobRunRequest,
                                   AtomicReference<software.amazon.awssdk.services.glue.model.JobRun> currentJobRun,
                                   Duration interval) {
        runContext.logger().debug("Waiting for job to reach stopped state...");

        Await.until(
            () -> pollAndUpdateJobState(glueClient, getJobRunRequest, runContext, currentJobRun),
            interval
        );
    }

    private boolean pollAndUpdateJobState(GlueClient glueClient, GetJobRunRequest getJobRunRequest,
                                          RunContext runContext,
                                          AtomicReference<software.amazon.awssdk.services.glue.model.JobRun> currentJobRun) {
        GetJobRunResponse jobRunResponse = glueClient.getJobRun(getJobRunRequest);
        software.amazon.awssdk.services.glue.model.JobRun jobRun = jobRunResponse.jobRun();
        currentJobRun.set(jobRun);

        runContext.logger().info("Job state: {}, Execution time: {} seconds",
            jobRun.jobRunStateAsString(), jobRun.executionTime());

        var state = jobRun.jobRunState();

        // Stop waiting when job reaches a terminal state
        return state.equals(software.amazon.awssdk.services.glue.model.JobRunState.STOPPED) ||
            state.equals(software.amazon.awssdk.services.glue.model.JobRunState.SUCCEEDED) ||
            state.equals(software.amazon.awssdk.services.glue.model.JobRunState.FAILED);
    }

    private Output buildOutput(String jobNameValue, String jobRunIdValue,
                               software.amazon.awssdk.services.glue.model.JobRun jobRun) {
        return Output.builder()
            .jobName(jobNameValue)
            .jobRunId(jobRunIdValue)
            .state(jobRun.jobRunStateAsString())
            .startedOn(ZonedDateTime.parse(jobRun.startedOn().toString()))
            .completedOn(jobRun.completedOn() != null ?
                ZonedDateTime.parse(jobRun.completedOn().toString()) : null)
            .lastModifiedOn(jobRun.lastModifiedOn() != null ?
                ZonedDateTime.parse(jobRun.lastModifiedOn().toString()) : null)
            .executionTime(jobRun.executionTime())
            .timeout(jobRun.timeout())
            .attempt(jobRun.attempt())
            .errorMessage(jobRun.errorMessage())
            .build();
    }
}
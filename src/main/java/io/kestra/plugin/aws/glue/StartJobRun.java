package io.kestra.plugin.aws.glue;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
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
import software.amazon.awssdk.services.glue.model.*;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.plugin.aws.glue.GlueService.createGetJobRunRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start an AWS Glue job and optionally wait for completion."
)
@Plugin(
    examples = {
        @Example(
            title = "Start a Glue job and wait for its completion",
            full = true,
            code = """
                id: aws_glue
                namespace: company.team

                tasks:
                  - id: start
                    type: io.kestra.plugin.aws.glue.StartJobRun
                    jobName: my-glue-job
                    maxDuration: 3600
                    wait: true
                    interval: 100
                """
        )
    }
)
public class StartJobRun extends AbstractGlueTask implements RunnableTask<Output> {

    @Schema(title = "The name of the Glue job to run.")
    @NotNull
    @PluginProperty
    private Property<String> jobName;

    @Schema(
        title = "The job arguments used for this job run.",
        description = "These are key-value string pairs passed to the job."
    )
    @PluginProperty
    private Property<Map<String, String>> arguments;

    @Schema(
        title = "Wait for the job to complete before ending the task.",
        description = "If true, the task will periodically check the job status until it completes."
    )
    @Builder.Default
    @PluginProperty
    private Property<Boolean> wait = Property.of(true);

    @Schema(
        title = "Timeout for waiting for job completion."
    )
    @PluginProperty
    private Property<Duration> maxDuration;

    @Schema(
        title = "Interval between status checks."
    )
    @Builder.Default
    @PluginProperty
    private Property<Duration> interval = Property.of(Duration.ofMillis(100));

    @Override
    public Output run(RunContext runContext) throws IllegalVariableEvaluationException {
        try (GlueClient glueClient = this.client(runContext)) {
            String jobNameValue = runContext.render(this.jobName).as(String.class).orElseThrow();
            String jobRunId = startJob(runContext, glueClient, jobNameValue);

            GetJobRunRequest getJobRunRequest = createGetJobRunRequest(jobNameValue, jobRunId);

            AtomicReference<JobRun> currentJobRun = new AtomicReference<>();
            GetJobRunResponse initialResponse = glueClient.getJobRun(getJobRunRequest);
            JobRun initialJobRun = initialResponse.jobRun();
            currentJobRun.set(initialJobRun);

            runContext.logger().info("Initial job state: {}", initialJobRun.jobRunStateAsString());

            if (runContext.render(this.wait).as(Boolean.class).orElse(true)) {
                waitForJobCompletion(runContext, glueClient, getJobRunRequest, currentJobRun);
            }

            return buildOutput(jobNameValue, jobRunId, currentJobRun.get());
        }
    }

    private String startJob(RunContext runContext, GlueClient glueClient, String jobNameValue) throws IllegalVariableEvaluationException {
        StartJobRunRequest.Builder requestBuilder = StartJobRunRequest.builder()
            .jobName(jobNameValue);

        if (this.maxDuration != null) {
            Duration duration = runContext.render(this.maxDuration).as(Duration.class).orElseThrow();
            requestBuilder.timeout((int) duration.toMinutes());
        }

        addArgumentsIfProvided(runContext, requestBuilder);

        StartJobRunResponse response = glueClient.startJobRun(requestBuilder.build());
        String jobRunId = response.jobRunId();

        runContext.logger().info("Started Glue job '{}' with run ID: {}",
            jobNameValue, jobRunId);
        return jobRunId;
    }

    private void addArgumentsIfProvided(RunContext runContext, StartJobRunRequest.Builder requestBuilder) throws IllegalVariableEvaluationException {
        if (arguments != null) {
            Map<String, String> renderedArgs = runContext.render(this.arguments).asMap(String.class, String.class);
            if (!renderedArgs.isEmpty()) {
                requestBuilder.arguments(renderedArgs);
            }
        }
    }

    private void waitForJobCompletion(RunContext runContext, GlueClient glueClient,
                                      GetJobRunRequest getJobRunRequest,
                                      AtomicReference<JobRun> currentJobRun) throws IllegalVariableEvaluationException {
        runContext.logger().debug("Waiting for job completion...");
        Duration intervalDuration = runContext.render(this.interval).as(Duration.class).orElseThrow();

        Await.until(
            () -> pollAndUpdateJobState(glueClient, getJobRunRequest, runContext, currentJobRun),
            intervalDuration
        );
    }

    private boolean pollAndUpdateJobState(GlueClient glueClient, GetJobRunRequest getJobRunRequest,
                                          RunContext runContext, AtomicReference<JobRun> currentJobRun) {
        GetJobRunResponse jobRunResponse = glueClient.getJobRun(getJobRunRequest);
        JobRun jobRun = jobRunResponse.jobRun();

        currentJobRun.set(jobRun);

        var state = jobRun.jobRunState();
        logJobCompletionStatus(runContext, jobRun);

        return !state.equals(JobRunState.STARTING) && !state.equals(JobRunState.RUNNING) &&
               !state.equals(JobRunState.WAITING);
    }

    private void logJobCompletionStatus(RunContext runContext, JobRun jobRun) {
        runContext.logger().info("Job state: {}", jobRun.jobRunStateAsString());
        if (jobRun.executionTime() != null) {
            runContext.logger().info("Execution time: {} seconds", jobRun.executionTime());
        }
    }

    private Output buildOutput(String jobNameValue, String jobRunId, JobRun jobRun) {
        return Output.builder()
            .jobName(jobNameValue)
            .jobRunId(jobRunId)
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
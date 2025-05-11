package io.kestra.plugin.aws.glue;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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
                    wait: true
                    checkFrequency:
                      maxIterations: 100
                      maxDuration: 3600
                      interval: 100
                """
        )
    }
)
public class StartJobRun extends AbstractGlueTask implements RunnableTask<StartJobRun.Output> {

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
        title = "Configuration for job status polling.",
        description = "Controls how often and how long the task will check for job completion."
    )
    @Builder.Default
    @PluginProperty
    private Property<CheckFrequencyConfig> checkFrequency = Property.of(CheckFrequencyConfig.builder()
        .maxIterations(100)
        .maxDuration(3600)
        .interval(100)
        .build());

    @Override
    public Output run(RunContext runContext) throws IllegalVariableEvaluationException {
        try (GlueClient glueClient = this.client(runContext)) {
            String jobNameValue = runContext.render(this.jobName).as(String.class).orElseThrow();
            String jobRunId = startJob(runContext, glueClient, jobNameValue);

            JobState jobState = new JobState();

            // Wait for job completion if requested
            if (runContext.render(this.wait).as(Boolean.class).orElse(true)) {
                waitForJobCompletion(runContext, glueClient, jobNameValue, jobRunId, jobState);
            }

            return buildOutput(jobNameValue, jobRunId, jobState);
        }
    }

    private String startJob(RunContext runContext, GlueClient glueClient, String jobNameValue) throws IllegalVariableEvaluationException {
        StartJobRunRequest.Builder requestBuilder = StartJobRunRequest.builder()
            .jobName(jobNameValue);

        addArgumentsIfProvided(runContext, requestBuilder);

        StartJobRunResponse response = glueClient.startJobRun(requestBuilder.build());
        String jobRunId = response.jobRunId();

        runContext.logger().info("Started Glue job '{}' with run ID: {}", jobNameValue, jobRunId);
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

    private void waitForJobCompletion(RunContext runContext, GlueClient glueClient, String jobNameValue,
                                      String jobRunId, JobState jobState) throws IllegalVariableEvaluationException {
        runContext.logger().info("Waiting for job completion...");

        CheckFrequencyConfig config = runContext.render(this.checkFrequency)
            .as(CheckFrequencyConfig.class)
            .orElseThrow();

        Duration interval = Duration.ofMillis(config.getInterval());
        Duration maxDuration = Duration.ofSeconds(config.getMaxDuration());

        try {
            Await.until(
                () -> pollAndUpdateJobState(glueClient, jobNameValue, jobRunId, jobState),
                interval,
                maxDuration
            );

            logJobCompletionStatus(runContext, jobState);
        } catch (TimeoutException e) {
            runContext.logger().warn("Timeout while waiting for Glue job to complete");
            throw new RuntimeException("Timeout waiting for Glue job to complete", e);
        }
    }

    private boolean pollAndUpdateJobState(GlueClient glueClient, String jobNameValue, String jobRunId, JobState jobState) {
        GetJobRunRequest getJobRunRequest = GetJobRunRequest.builder()
            .jobName(jobNameValue)
            .runId(jobRunId)
            .build();

        GetJobRunResponse jobRunResponse = glueClient.getJobRun(getJobRunRequest);
        JobRun jobRun = jobRunResponse.jobRun();

        if (jobRun != null) {
            updateJobState(jobRun, jobState);
        }

        return !jobState.state.get().equals("STARTING") && !jobState.state.get().equals("RUNNING");
    }

    private void updateJobState(JobRun jobRun, JobState jobState) {
        jobState.state.set(jobRun.jobRunStateAsString());

        Optional.ofNullable(jobRun.startedOn())
            .ifPresent(time -> jobState.startedOn.set(ZonedDateTime.parse(time.toString())));

        Optional.ofNullable(jobRun.completedOn())
            .ifPresent(time -> jobState.completedOn.set(ZonedDateTime.parse(time.toString())));

        Optional.ofNullable(jobRun.lastModifiedOn())
            .ifPresent(time -> jobState.lastModifiedOn.set(ZonedDateTime.parse(time.toString())));

        jobState.executionTime.set(jobRun.executionTime());
        jobState.timeout.set(jobRun.timeout());
        jobState.attempt.set(jobRun.attempt());
        jobState.errorMessage.set(jobRun.errorMessage());
    }

    private void logJobCompletionStatus(RunContext runContext, JobState jobState) {
        if (!jobState.state.get().equals("SUCCEEDED")) {
            runContext.logger().warn("Glue job completed with non-success state: {}", jobState.state.get());
            if (jobState.errorMessage.get() != null && !jobState.errorMessage.get().isEmpty()) {
                runContext.logger().warn("Error message: {}", jobState.errorMessage.get());
            }
        }
    }

    private Output buildOutput(String jobNameValue, String jobRunId, JobState jobState) {
        return Output.builder()
            .jobName(jobNameValue)
            .jobRunId(jobRunId)
            .state(jobState.state.get())
            .startedOn(jobState.startedOn.get())
            .completedOn(jobState.completedOn.get())
            .lastModifiedOn(jobState.lastModifiedOn.get())
            .executionTime(jobState.executionTime.get())
            .timeout(jobState.timeout.get())
            .attempt(jobState.attempt.get())
            .errorMessage(jobState.errorMessage.get())
            .build();
    }

    private static class JobState {
        final AtomicReference<String> state = new AtomicReference<>("STARTING");
        final AtomicReference<ZonedDateTime> startedOn = new AtomicReference<>(null);
        final AtomicReference<ZonedDateTime> completedOn = new AtomicReference<>(null);
        final AtomicReference<ZonedDateTime> lastModifiedOn = new AtomicReference<>(null);
        final AtomicReference<Integer> executionTime = new AtomicReference<>(null);
        final AtomicReference<Integer> timeout = new AtomicReference<>(null);
        final AtomicReference<Integer> attempt = new AtomicReference<>(null);
        final AtomicReference<String> errorMessage = new AtomicReference<>(null);
    }

    @SuperBuilder
    @ToString
    @Getter
    @Schema(title = "Check frequency configuration")
    public static class CheckFrequencyConfig {
        @Schema(title = "Maximum number of iterations to check job status (not directly used)")
        @Builder.Default
        private Integer maxIterations = 100;

        @Schema(title = "Maximum duration to wait for job completion (in seconds)")
        @Builder.Default
        private Integer maxDuration = 3600;

        @Schema(title = "Interval between status checks (in milliseconds)")
        @Builder.Default
        private Integer interval = 100;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The name of the job")
        private final String jobName;

        @Schema(title = "The ID of the job run")
        private final String jobRunId;

        @Schema(title = "The current state of the job run")
        private final String state;

        @Schema(title = "When the job run was started")
        private final ZonedDateTime startedOn;

        @Schema(title = "When the job run was completed, if applicable")
        private final ZonedDateTime completedOn;

        @Schema(title = "The last time the job run was modified")
        private final ZonedDateTime lastModifiedOn;

        @Schema(title = "The execution time of the job in seconds")
        private final Integer executionTime;

        @Schema(title = "The timeout configuration for the job in minutes")
        private final Integer timeout;

        @Schema(title = "The attempt number for this job run")
        private final Integer attempt;

        @Schema(title = "The error message if the job failed")
        private final String errorMessage;
    }
}
package io.kestra.plugin.aws.glue;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
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
                    maxDuration: PT1H
                    wait: true
                    interval: 100
                """
        )
    }
)
public class StartJobRun extends AbstractGlueTask implements RunnableTask<Output> {

    @Schema(title = "The name of the Glue job to run.")
    @NotNull
    private Property<String> jobName;

    @Schema(
        title = "The job arguments used for this job run.",
        description = "These are key-value string pairs passed to the job."
    )
    private Property<Map<String, String>> arguments;

    @Schema(
        title = "Wait for the job to complete before ending the task.",
        description = "If true, the task will periodically check the job status until it completes."
    )
    @Builder.Default
    private Property<Boolean> wait = Property.of(true);

    @Schema(
        title = "Timeout for waiting for job completion.",
        description = "If the job does not complete within this duration (rounded up to minutes), the task will fail. " +
                      "If this property is not set, the default timeout is 480 minutes (8 hours) for Glue 5.0 ETL jobs, 2,880 minutes (48 hours) for Glue 4.0 and below, " +
                      "and no job timeout is defaulted for a Glue Streaming job."
    )
    private Property<Duration> maxDuration;

    @Schema(
        title = "Interval between status checks."
    )
    @Builder.Default
    private Property<Duration> interval = Property.of(Duration.ofMillis(1000));

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

            if (!currentJobRun.get().jobRunState().equals(JobRunState.SUCCEEDED) && !currentJobRun.get().jobRunState().equals(JobRunState.RUNNING)
                && !currentJobRun.get().jobRunState().equals(JobRunState.WAITING) && !currentJobRun.get().jobRunState().equals(JobRunState.STARTING)) {
                throw new RuntimeException("Job state: " + currentJobRun.get().jobRunStateAsString() +
                                           (currentJobRun.get().errorMessage() != null ? ", Error message: " + currentJobRun.get().errorMessage() : ""));
            }

            return buildOutput(jobNameValue, jobRunId, currentJobRun.get());
        }
    }

    private String startJob(RunContext runContext, GlueClient glueClient, String jobNameValue) throws IllegalVariableEvaluationException {
        StartJobRunRequest.Builder requestBuilder = StartJobRunRequest.builder()
            .jobName(jobNameValue);

        if (this.maxDuration != null) {
            Duration duration = runContext.render(this.maxDuration).as(Duration.class).orElseThrow();
            BigDecimal mins = BigDecimal
                .valueOf(duration.toMillis())
                .divide(BigDecimal.valueOf(60_000), RoundingMode.UP);
            requestBuilder.timeout(mins.intValue());
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

        runContext.logger().info("Job state: {}, Execution time: {} seconds", jobRun.jobRunStateAsString(), jobRun.executionTime());

        var state = jobRun.jobRunState();

        return !state.equals(JobRunState.STARTING) && !state.equals(JobRunState.RUNNING) &&
               !state.equals(JobRunState.WAITING);
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
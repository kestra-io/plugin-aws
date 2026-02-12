package io.kestra.plugin.aws.glue;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.glue.model.Output;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import java.time.ZonedDateTime;
import java.util.Comparator;

import static io.kestra.plugin.aws.glue.GlueService.createGetJobRunRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get Glue job run status",
    description = "Retrieves details for a specific Glue job run; if runId is absent, fetches the latest run for the job."
)
@Plugin(
    examples = {
        @Example(
            title = "Check the status of a specific Glue job run",
            full = true,
            code = """
                id: check_glue_job
                namespace: company.team

                tasks:
                  - id: get_job_status
                    type: io.kestra.plugin.aws.glue.GetJobRun
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    jobName: sample-data-flight-etl
                    runId: "{{ outputs.start_job.jobRunId }}"
                """
        ),
        @Example(
            title = "Check the status of the latest Glue job run",
            full = true,
            code = """
                id: check_latest_glue_job
                namespace: company.team

                tasks:
                  - id: get_latest_job_status
                    type: io.kestra.plugin.aws.glue.GetJobRun
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    jobName: sample-data-flight-etl
                """
        )
    }
)
public class GetJobRun extends AbstractGlueTask implements RunnableTask<Output> {
    @Schema(
        title = "Job name",
        description = "Glue job whose run status is requested."
    )
    @NotNull
    private Property<String> jobName;

    @Schema(
        title = "Run ID",
        description = "Specific run ID; when omitted, the latest run is selected."
    )
    private Property<String> runId;

    @Override
    public Output run(RunContext runContext) throws IllegalVariableEvaluationException {
        try (GlueClient glueClient = this.glueClient(runContext)) {
            String jobNameValue = runContext.render(this.jobName).as(String.class).orElseThrow();
            String runIdValue = null;

            if (this.runId != null) {
                runIdValue = runContext.render(this.runId).as(String.class).orElse(null);
            }

            if (runIdValue == null || runIdValue.isEmpty()) {
                runContext.logger().debug("No runId provided, retrieving the latest job run for job '{}'", jobNameValue);

                GetJobRunsRequest jobRunsRequest = GetJobRunsRequest.builder()
                    .jobName(jobNameValue)
                    .maxResults(10)
                    .build();

                GetJobRunsResponse jobRunsResponse = glueClient.getJobRuns(jobRunsRequest);

                if (jobRunsResponse.jobRuns() == null || jobRunsResponse.jobRuns().isEmpty()) {
                    throw new IllegalArgumentException("No job runs found for job: " + jobNameValue);
                }

                JobRun latestRun = jobRunsResponse.jobRuns().stream()
                    .max(Comparator.comparing(JobRun::startedOn))
                    .orElseThrow(() -> new IllegalArgumentException("Failed to determine latest job run"));

                runIdValue = latestRun.id();
                runContext.logger().info("Using latest job run with ID: {}", runIdValue);
            }

            GetJobRunRequest request = createGetJobRunRequest(jobNameValue, runIdValue);

            GetJobRunResponse response = glueClient.getJobRun(request);

            runContext.logger().debug(
                "Glue job '{}' run '{}' is in state: {}",
                jobNameValue,
                runIdValue,
                response.jobRun().jobRunStateAsString()
            );

            return Output.builder()
                .jobName(response.jobRun().jobName())
                .jobRunId(response.jobRun().id())
                .state(response.jobRun().jobRunStateAsString())
                .startedOn(ZonedDateTime.parse(response.jobRun().startedOn().toString()))
                .completedOn(response.jobRun().completedOn() != null ?
                    ZonedDateTime.parse(response.jobRun().completedOn().toString()) : null)
                .lastModifiedOn(response.jobRun().lastModifiedOn() != null ?
                    ZonedDateTime.parse(response.jobRun().lastModifiedOn().toString()) : null)
                .executionTime(response.jobRun().executionTime())
                .timeout(response.jobRun().timeout())
                .attempt(response.jobRun().attempt())
                .errorMessage(response.jobRun().errorMessage())
                .build();
        }
    }

}

package io.kestra.plugin.aws.healthlake;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.healthlake.HealthLakeClient;
import software.amazon.awssdk.services.healthlake.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow when a HealthLake import or export job reaches a terminal state",
    description = """
        Polls HealthLake import or export jobs at a fixed interval and fires when the most recently submitted
        job for the given data store reaches a terminal state (`COMPLETED`, `COMPLETED_WITH_ERRORS`, or `FAILED`).
        Exposes `{{ trigger.jobId }}` and `{{ trigger.jobStatus }}` to downstream tasks.
        Note: `listFHIRImportJobs`/`listFHIRExportJobs` with `maxResults(1)` returns the most recently submitted job
        but does not guarantee ordering by submit time; use `submittedAfter` filtering for strict ordering requirements.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a flow when a FHIR import job completes.",
            full = true,
            code = """
                id: on_fhir_import_complete
                namespace: company.team

                triggers:
                  - id: import_done
                    type: io.kestra.plugin.aws.healthlake.Trigger
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    datastoreId: "{{ secret('HEALTHLAKE_DATASTORE_ID') }}"
                    jobType: IMPORT
                    interval: PT2M

                tasks:
                  - id: notify
                    type: io.kestra.plugin.core.log.Log
                    message: "FHIR import job {{ trigger.jobId }} finished with status {{ trigger.jobStatus }}"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, AbstractConnectionInterface {

    @Schema(title = "AWS access key ID", description = "Optional static credential. If omitted, the default credentials provider chain is used.")
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> accessKeyId;

    @Schema(title = "AWS secret access key", description = "Pairs with `accessKeyId` for static credentials.")
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> secretKeyId;

    @Schema(title = "AWS session token", description = "Session token for temporary credentials.")
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> sessionToken;

    @Schema(title = "AWS region", description = "The AWS region for the HealthLake service.")
    @PluginProperty(group = "connection")
    protected Property<String> region;

    @Schema(title = "Endpoint override", description = "Override the default AWS endpoint URL.")
    @PluginProperty(group = "connection")
    protected Property<String> endpointOverride;

    @Schema(title = "STS role ARN", description = "IAM role ARN to assume via STS before making API calls.")
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> stsRoleArn;

    @Schema(title = "STS role external ID", description = "External ID to pass when assuming the STS role.")
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> stsRoleExternalId;

    @Schema(title = "STS role session name", description = "Session name to use when assuming the STS role.")
    @PluginProperty(group = "connection")
    protected Property<String> stsRoleSessionName;

    @Schema(title = "STS endpoint override", description = "Override the default STS endpoint URL.")
    @PluginProperty(group = "connection")
    protected Property<String> stsEndpointOverride;

    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    @Schema(title = "Poll interval", description = "How often to poll HealthLake for job status. ISO-8601 duration, e.g. `PT2M` for every 2 minutes.")
    @PluginProperty(group = "main")
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(120);

    @Schema(title = "Data store ID", description = "The HealthLake data store to monitor.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> datastoreId;

    @Schema(
        title = "Job type",
        description = "Whether to monitor `IMPORT` or `EXPORT` jobs. Default is `IMPORT`."
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private Property<JobType> jobType = Property.ofValue(JobType.IMPORT);

    private static final List<String> TERMINAL_STATUSES = List.of(
        "COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED", "CANCEL_COMPLETED", "CANCEL_FAILED"
    );

    private static final String STATE_FILE = "healthlake-trigger-last-job-id.txt";

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        var resolvedDatastoreId = runContext.render(datastoreId).as(String.class).orElseThrow();
        var resolvedJobType = runContext.render(jobType).as(JobType.class).orElse(JobType.IMPORT);

        // Load last-fired jobId to avoid re-firing on the same terminal job every poll
        String lastFiredJobId = null;
        try {
            var stateFile = runContext.storage().getTaskStateFile(STATE_FILE, this.getId(), false);
            if (stateFile != null) {
                try (var reader = new BufferedReader(new InputStreamReader(stateFile, StandardCharsets.UTF_8))) {
                    lastFiredJobId = reader.readLine();
                }
            }
        } catch (Exception e) {
            logger.debug("No previous trigger state found, treating as first run");
        }

        var clientConfig = awsClientConfig(runContext);

        try (var client = ConnectionUtils.configureSyncClient(clientConfig, HealthLakeClient.builder()).build()) {
            String jobId = null;
            String jobStatus = null;

            if (resolvedJobType == JobType.IMPORT) {
                var response = client.listFHIRImportJobs(ListFHIRImportJobsRequest.builder()
                    .datastoreId(resolvedDatastoreId)
                    .maxResults(1)
                    .build());
                var jobs = response.importJobPropertiesList();
                if (!jobs.isEmpty()) {
                    jobId = jobs.getFirst().jobId();
                    jobStatus = jobs.getFirst().jobStatus().toString();
                }
            } else {
                var response = client.listFHIRExportJobs(ListFHIRExportJobsRequest.builder()
                    .datastoreId(resolvedDatastoreId)
                    .maxResults(1)
                    .build());
                var jobs = response.exportJobPropertiesList();
                if (!jobs.isEmpty()) {
                    jobId = jobs.getFirst().jobId();
                    jobStatus = jobs.getFirst().jobStatus().toString();
                }
            }

            if (jobId == null) {
                logger.debug("No {} jobs found for datastore '{}'", resolvedJobType, resolvedDatastoreId);
                return Optional.empty();
            }

            if (!TERMINAL_STATUSES.contains(jobStatus)) {
                logger.debug("Job '{}' not yet terminal, status={}", jobId, jobStatus);
                return Optional.empty();
            }

            // Deduplication: skip if this is the same job we already fired on
            if (jobId.equals(lastFiredJobId)) {
                logger.debug("Job '{}' already fired, skipping", jobId);
                return Optional.empty();
            }

            // Persist the jobId so we don't fire on it again
            var bytes = jobId.getBytes(StandardCharsets.UTF_8);
            runContext.storage().putTaskStateFile(new ByteArrayInputStream(bytes), STATE_FILE, this.getId(), false);

            logger.debug("Job '{}' reached terminal status={}, firing trigger", jobId, jobStatus);
            var output = Output.builder().jobId(jobId).jobStatus(jobStatus).build();
            return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
        }
    }

    public enum JobType {
        IMPORT, EXPORT
    }

    @SuperBuilder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Job ID", description = "The ID of the job that reached a terminal state.")
        private final String jobId;

        @Schema(title = "Job status", description = "Terminal status: `COMPLETED`, `COMPLETED_WITH_ERRORS`, `FAILED`, `CANCEL_COMPLETED`, or `CANCEL_FAILED`.")
        private final String jobStatus;
    }
}

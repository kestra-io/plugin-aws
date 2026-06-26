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

    @Schema(title = "AWS access key ID.", description = "Optional static credential. If omitted, the default credentials provider chain is used.")
    @PluginProperty(secret = true, group = "advanced")
    protected Property<String> accessKeyId;

    @Schema(title = "AWS secret access key.", description = "Pairs with `accessKeyId` for static credentials.")
    @PluginProperty(secret = true, group = "advanced")
    protected Property<String> secretKeyId;

    @Schema(title = "AWS session token for temporary credentials.")
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> sessionToken;

    protected Property<String> region;
    protected Property<String> endpointOverride;
    protected Property<String> stsRoleArn;
    protected Property<String> stsRoleExternalId;
    protected Property<String> stsRoleSessionName;
    protected Property<String> stsEndpointOverride;

    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

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

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        var resolvedDatastoreId = runContext.render(datastoreId).as(String.class).orElseThrow();
        var resolvedJobType = runContext.render(jobType).as(JobType.class).orElse(JobType.IMPORT);

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

            logger.debug("Job '{}' reached terminal status={}", jobId, jobStatus);
            var output = Output.builder().jobId(jobId).jobStatus(jobStatus).build();
            return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
        }
    }

    public enum JobType {
        IMPORT, EXPORT
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Job ID", description = "The ID of the job that reached a terminal state.")
        private final String jobId;

        @Schema(title = "Job status", description = "Terminal status: `COMPLETED`, `COMPLETED_WITH_ERRORS`, `FAILED`, `CANCEL_COMPLETED`, or `CANCEL_FAILED`.")
        private final String jobStatus;
    }
}

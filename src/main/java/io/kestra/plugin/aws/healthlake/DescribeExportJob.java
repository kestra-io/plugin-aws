package io.kestra.plugin.aws.healthlake;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.healthlake.HealthLakeClient;
import software.amazon.awssdk.services.healthlake.model.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Describe an Amazon HealthLake FHIR export job",
    description = """
        Returns the current status, timing, output location, and error details of a HealthLake bulk export job.
        Pair with Kestra's retry/wait pattern or use the `Trigger` task to poll for completion automatically.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Poll the status of a HealthLake export job.",
            full = true,
            code = """
                id: healthlake_describe_export
                namespace: company.team

                tasks:
                  - id: describe_export
                    type: io.kestra.plugin.aws.healthlake.DescribeExportJob
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    datastoreId: "{{ secret('HEALTHLAKE_DATASTORE_ID') }}"
                    jobId: "{{ inputs.job_id }}"

                  - id: log_status
                    type: io.kestra.plugin.core.log.Log
                    message: "Export job status: {{ outputs.describe_export.jobStatus }}"
                """
        )
    }
)
public class DescribeExportJob extends AbstractConnection implements RunnableTask<DescribeExportJob.Output> {

    @Schema(title = "Data store ID", description = "The ID of the FHIR data store the export job belongs to.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> datastoreId;

    @Schema(title = "Job ID", description = "The ID of the export job to describe.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> jobId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var rDatastoreId = runContext.render(datastoreId).as(String.class).orElseThrow();
        var rJobId = runContext.render(jobId).as(String.class).orElseThrow();

        var request = DescribeFhirExportJobRequest.builder()
            .datastoreId(rDatastoreId)
            .jobId(rJobId)
            .build();

        logger.debug("Describing HealthLake export job '{}'", rJobId);

        try (var client = client(runContext)) {
            var props = client.describeFHIRExportJob(request).exportJobProperties();
            String outputS3Uri = null;
            if (props.outputDataConfig() != null && props.outputDataConfig().s3Configuration() != null) {
                outputS3Uri = props.outputDataConfig().s3Configuration().s3Uri();
            }
            return Output.builder()
                .jobId(props.jobId())
                .jobName(props.jobName())
                .jobStatus(props.jobStatus().toString())
                .datastoreId(props.datastoreId())
                .submitTime(props.submitTime() != null ? props.submitTime().toString() : null)
                .endTime(props.endTime() != null ? props.endTime().toString() : null)
                .outputS3Uri(outputS3Uri)
                .message(props.message())
                .build();
        }
    }

    @VisibleForTesting
    HealthLakeClient client(RunContext runContext) throws Exception {
        var clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, HealthLakeClient.builder()).build();
    }

    @SuperBuilder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Job ID")
        private final String jobId;

        @Schema(title = "Job name")
        private final String jobName;

        @Schema(
            title = "Job status",
            description = "One of: `SUBMITTED`, `IN_PROGRESS`, `COMPLETED_WITH_ERRORS`, `COMPLETED`, `FAILED`, `CANCEL_SUBMITTED`, `CANCEL_IN_PROGRESS`, `CANCEL_COMPLETED`, `CANCEL_FAILED`."
        )
        private final String jobStatus;

        @Schema(title = "Data store ID")
        private final String datastoreId;

        @Schema(title = "Submit time", description = "ISO-8601 timestamp when the job was submitted.")
        private final String submitTime;

        @Schema(title = "End time", description = "ISO-8601 timestamp when the job finished (null if still running).")
        private final String endTime;

        @Schema(title = "Output S3 URI", description = "S3 URI where the exported FHIR bundles were written.")
        private final String outputS3Uri;

        @Schema(title = "Message", description = "Error or informational message from the service.")
        private final String message;
    }
}

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

import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start an Amazon HealthLake FHIR bulk export job",
    description = """
        Submits a bulk FHIR export job from a HealthLake data store to an S3 destination and returns the job ID.
        Use `DescribeExportJob` to poll for completion, or use the `Trigger` task to react when it finishes.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Start a FHIR bulk export to S3.",
            full = true,
            code = """
                id: healthlake_start_export
                namespace: company.team

                inputs:
                  - id: datastore_id
                    type: STRING

                tasks:
                  - id: start_export
                    type: io.kestra.plugin.aws.healthlake.StartExportJob
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    datastoreId: "{{ inputs.datastore_id }}"
                    outputS3Uri: "s3://my-bucket/healthlake-export/"
                    dataAccessRoleArn: "{{ secret('HEALTHLAKE_ROLE_ARN') }}"

                  - id: log_job
                    type: io.kestra.plugin.core.log.Log
                    message: "Export job started: {{ outputs.start_export.jobId }}"
                """
        )
    }
)
public class StartExportJob extends AbstractConnection implements RunnableTask<StartExportJob.Output> {

    @Schema(title = "Data store ID", description = "The ID of the FHIR data store to export from.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> datastoreId;

    @Schema(title = "Output S3 URI", description = "S3 URI prefix where the exported FHIR bundles are written.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> outputS3Uri;

    @Schema(title = "Data access role ARN", description = "IAM role ARN that HealthLake assumes to write to the output S3 bucket.")
    @PluginProperty(group = "main", secret = true)
    @NotNull
    private Property<String> dataAccessRoleArn;

    @Schema(title = "KMS key ID", description = "Optional KMS key ID for server-side encryption of the export output.")
    @PluginProperty(group = "processing")
    private Property<String> kmsKeyId;

    @Schema(title = "Job name", description = "Optional human-readable name for the export job.")
    @PluginProperty(group = "processing")
    private Property<String> jobName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rDatastoreId = runContext.render(datastoreId).as(String.class).orElseThrow();
        var rOutputUri = runContext.render(outputS3Uri).as(String.class).orElseThrow();
        var rRoleArn = runContext.render(dataAccessRoleArn).as(String.class).orElseThrow();

        var s3ConfigBuilder = S3Configuration.builder().s3Uri(rOutputUri);
        if (kmsKeyId != null) {
            s3ConfigBuilder.kmsKeyId(runContext.render(kmsKeyId).as(String.class).orElse(null));
        }

        var requestBuilder = StartFHIRExportJobRequest.builder()
            .datastoreId(rDatastoreId)
            .outputDataConfig(OutputDataConfig.builder()
                .s3Configuration(s3ConfigBuilder.build())
                .build())
            .dataAccessRoleArn(rRoleArn)
            .clientToken(UUID.randomUUID().toString());

        if (jobName != null) {
            requestBuilder.jobName(runContext.render(jobName).as(String.class).orElse(null));
        }

        logger.debug("Starting HealthLake export job for datastore '{}'", rDatastoreId);

        try (var client = client(runContext)) {
            var response = client.startFHIRExportJob(requestBuilder.build());
            logger.debug("Export job started: jobId={} status={}", response.jobId(), response.jobStatus());
            return Output.builder()
                .jobId(response.jobId())
                .jobStatus(response.jobStatus().toString())
                .datastoreId(response.datastoreId())
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

        @Schema(title = "Job ID", description = "The ID of the submitted export job.")
        private final String jobId;

        @Schema(title = "Job status", description = "Initial job status, typically `SUBMITTED`.")
        private final String jobStatus;

        @Schema(title = "Data store ID", description = "The data store the job was submitted from.")
        private final String datastoreId;
    }
}

package io.kestra.plugin.aws.healthlake;

import java.util.UUID;

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
    title = "Start an Amazon HealthLake FHIR bulk import job",
    description = """
        Submits a bulk FHIR import job from an S3 URI into a HealthLake data store and returns the job ID.
        Use `DescribeImportJob` to poll for completion, or use the `Trigger` task to react when it finishes.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Start a FHIR bulk import from S3.",
            full = true,
            code = """
                id: healthlake_start_import
                namespace: company.team

                inputs:
                  - id: datastore_id
                    type: STRING
                  - id: s3_input_uri
                    type: STRING

                tasks:
                  - id: start_import
                    type: io.kestra.plugin.aws.healthlake.StartImportJob
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    datastoreId: "{{ inputs.datastore_id }}"
                    inputS3Uri: "{{ inputs.s3_input_uri }}"
                    outputS3Uri: "s3://my-bucket/healthlake-output/"
                    dataAccessRoleArn: "{{ secret('HEALTHLAKE_ROLE_ARN') }}"

                  - id: log_job
                    type: io.kestra.plugin.core.log.Log
                    message: "Import job started: {{ outputs.start_import.jobId }}"
                """
        )
    }
)
public class StartImportJob extends AbstractConnection implements RunnableTask<StartImportJob.Output> {

    @Schema(title = "Data store ID", description = "The ID of the target FHIR data store.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> datastoreId;

    @Schema(title = "Input S3 URI", description = "S3 URI of the FHIR bundle(s) to import, e.g. `s3://my-bucket/fhir/`.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> inputS3Uri;

    @Schema(title = "Output S3 URI", description = "S3 URI prefix where import job output and error files are written.")
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> outputS3Uri;

    @Schema(title = "Data access role ARN", description = "IAM role ARN that HealthLake assumes to read from the input S3 bucket and write to the output bucket.")
    @PluginProperty(group = "connection", secret = true)
    @NotNull
    private Property<String> dataAccessRoleArn;

    @Schema(title = "Job name", description = "Optional human-readable name for the import job.")
    @PluginProperty(group = "processing")
    private Property<String> jobName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rDatastoreId = runContext.render(datastoreId).as(String.class).orElseThrow();
        var rInputUri = runContext.render(inputS3Uri).as(String.class).orElseThrow();
        var rOutputUri = runContext.render(outputS3Uri).as(String.class).orElseThrow();
        var rRoleArn = runContext.render(dataAccessRoleArn).as(String.class).orElseThrow();

        var requestBuilder = StartFhirImportJobRequest.builder()
            .datastoreId(rDatastoreId)
            .inputDataConfig(InputDataConfig.fromS3Uri(rInputUri))
            .jobOutputDataConfig(
                OutputDataConfig.builder()
                    .s3Configuration(
                        S3Configuration.builder()
                            .s3Uri(rOutputUri)
                            .build()
                    )
                    .build()
            )
            .dataAccessRoleArn(rRoleArn)
            .clientToken(UUID.randomUUID().toString());

        if (jobName != null) {
            requestBuilder.jobName(runContext.render(jobName).as(String.class).orElse(null));
        }

        logger.debug("Starting HealthLake import job for datastore '{}'", rDatastoreId);

        try (var client = client(runContext)) {
            var response = client.startFHIRImportJob(requestBuilder.build());
            logger.debug("Import job started: jobId={} status={}", response.jobId(), response.jobStatus());
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

        @Schema(title = "Job ID", description = "The ID of the submitted import job.")
        private final String jobId;

        @Schema(title = "Job status", description = "Initial job status, typically `SUBMITTED`.")
        private final String jobStatus;

        @Schema(title = "Data store ID", description = "The data store the job was submitted to.")
        private final String datastoreId;
    }
}

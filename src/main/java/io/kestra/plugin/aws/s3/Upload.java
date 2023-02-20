package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "from: \"{{ inputs.file }}\"",
                "bucket: \"my-bucket\"",
                "key: \"path/to/file\""
            }
        )
    }
)
@Schema(
    title = "Upload a file to a S3 bucket."
)
public class Upload extends AbstractS3Object implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file to upload"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Schema(
        title = "The key where to upload the file"
    )
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "A map of metadata to store with the object in S3."
    )
    @PluginProperty
    private Map<String, String> metadata;

    @Schema(
        title = "If you don't specify, S3 Standard is the default storage class. Amazon S3 supports other storage classes."
    )
    @PluginProperty(dynamic = true)
    private String storageClass;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket);
        String key = runContext.render(this.key);

        try (S3AsyncClient client = this.asyncClient(runContext)) {
            File tempFile = runContext.tempFile().toFile();
            URI from = new URI(runContext.render(this.from));
            Files.copy(runContext.uriToInputStream(from), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            PutObjectRequest.Builder builder = PutObjectRequest
                .builder()
                .bucket(bucket)
                .key(key);

            if (this.requestPayer != null) {
                builder.requestPayer(runContext.render(this.requestPayer));
            }

            if (this.metadata != null) {
                builder.metadata(this.metadata);
            }

            if (this.storageClass != null) {
                builder.storageClass(runContext.render(this.storageClass));
            }

            // use the transfer manager for uploading an S3 file will end up using 8MB upload parts.
            try (S3TransferManager transferManager = S3TransferManager.builder().s3Client(client).build()) {
                FileUpload upload = transferManager.uploadFile(UploadFileRequest.builder()
                    .putObjectRequest(builder.build())
                    .source(tempFile)
                    .build());

                // wait for the upload
                PutObjectResponse response = upload.completionFuture().get().response();

                runContext.metric(Counter.of("file.size", tempFile.length()));
                return Output
                    .builder()
                    .bucket(bucket)
                    .key(key)
                    .eTag(response.eTag())
                    .versionId(response.versionId())
                    .build();
            }
        }

    }

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        private final String bucket;
        private final String key;
    }
}

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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

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
    @PluginProperty(dynamic = false)
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

        try (S3Client client = this.client(runContext)) {
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

            PutObjectResponse response = client.putObject(
                builder.build(),
                RequestBody.fromFile(tempFile)
            );
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

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        private final String bucket;
        private final String key;
    }
}

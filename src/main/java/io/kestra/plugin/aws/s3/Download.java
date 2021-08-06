package io.kestra.plugin.aws.s3;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.File;
import java.net.URI;
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
                "bucket: \"my-bucket\"",
                "key: \"path/to/file\""
            }
        )
    }
)
@Schema(
    title = "Download a file to a S3 bucket."
)
public class Download extends AbstractS3Object implements RunnableTask<Download.Output> {
    @Schema(
        title = "The bucket where to download the file"
    )
    @PluginProperty(dynamic = true)
    private String bucket;

    @Schema(
        title = "The key where to download the file"
    )
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "VersionId used to reference a specific version of the object."
    )
    @PluginProperty(dynamic = true)
    protected String versionId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket);
        String key = runContext.render(this.key);

        // s3 require non existing files
        File tempFile = runContext.tempFile().toFile();
        //noinspection ResultOfMethodCallIgnored
        tempFile.delete();

        try (S3Client client = this.client(runContext)) {
            GetObjectRequest.Builder builder = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key);

            if (this.versionId != null) {
                builder.versionId(runContext.render(this.versionId));
            }

            if (this.requestPayer != null) {
                builder.requestPayer(runContext.render(this.requestPayer));
            }

            GetObjectResponse response = client.getObject(
                builder.build(),
                ResponseTransformer.toFile(tempFile)
            );

            runContext.metric(Counter.of("file.size", response.contentLength()));

            return Output
                .builder()
                .uri(runContext.putTempFile(tempFile))
                .eTag(response.eTag())
                .contentLength(response.contentLength())
                .contentType(response.contentType())
                .metadata(response.metadata())
                .versionId(response.versionId())
                .build();
        }
    }

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        private final URI uri;

        @Schema(
            title = "Size of the body in bytes."
        )
        private final Long contentLength;

        @Schema(
            title = "A standard MIME type describing the format of the object data."
        )
        private final String contentType;

        @Schema(
            title = "A map of metadata to store with the object in S3."
        )
        private final Map<String, String> metadata;
    }
}

package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

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
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "bucket: \"my-bucket\"",
                "key: \"path/to/file\""
            }
        )
    }
)
@Schema(
    title = "Download a file from an S3 bucket."
)
public class Download extends AbstractS3Object implements RunnableTask<Download.Output> {
    @Schema(
        title = "The key of a file to download."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String key;

    @Schema(
        title = "The specific version of the object."
    )
    @PluginProperty(dynamic = true)
    protected String versionId;

    @Schema(
        title = "If set to true, the task will use the AWS S3 DefaultAsyncClient instead of the S3CrtAsyncClient, which better integrates with S3-compatible services but restricts uploads and downloads to 2GB."
    )
    @PluginProperty
    @Builder.Default
    private Boolean compatibilityMode = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket);
        String key = runContext.render(this.key);

        try (S3AsyncClient client = this.asyncClient(runContext)) {
            GetObjectRequest.Builder builder = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key);

            if (this.versionId != null) {
                builder.versionId(runContext.render(this.versionId));
            }

            if (this.requestPayer != null) {
                builder.requestPayer(runContext.render(this.requestPayer));
            }

            Pair<GetObjectResponse, URI> download = S3Service.download(runContext, client, builder.build());

            return Output
                .builder()
                .uri(download.getRight())
                .eTag(download.getLeft().eTag())
                .contentLength(download.getLeft().contentLength())
                .contentType(download.getLeft().contentType())
                .metadata(download.getLeft().metadata())
                .versionId(download.getLeft().versionId())
                .build();
        }
    }

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        private final URI uri;

        @Schema(
            title = "The size of the body in bytes."
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

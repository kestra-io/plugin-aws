package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;

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
                "from:",
                "  bucket: \"my-bucket\"",
                "  key: \"path/to/file\"",
                "to:",
                "  bucket: \"my-bucket2\"",
                "  key: \"path/to/file2\"",
            }
        )
    }
)
@Schema(
    title = "Copy a file between S3 bucket."
)
public class Copy extends AbstractS3 implements RunnableTask<Copy.Output> {
    @Schema(
        title = "The source bucket and key"
    )
    @PluginProperty(dynamic = false)
    private CopyObjectFrom from;

    @Schema(
        title = "The destination bucket and key."
    )
    @PluginProperty(dynamic = false)
    private CopyObject to;

    @Schema(
        title = "Delete the source file after download"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean delete = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (S3Client client = this.client(runContext)) {
            CopyObjectRequest.Builder builder = CopyObjectRequest.builder()
                .sourceBucket(runContext.render(this.from.bucket))
                .sourceKey(runContext.render(this.from.key))
                .destinationBucket(runContext.render(this.to.bucket != null ? this.to.bucket : this.from.bucket))
                .destinationKey(runContext.render(this.to.key != null ? this.to.key : this.from.key));

            if (this.from.versionId != null) {
                builder.sourceVersionId(runContext.render(this.from.versionId));
            }

            CopyObjectRequest request = builder.build();
            CopyObjectResponse response = client.copyObject(request);

            if (this.delete) {
                Delete.builder()
                    .id(this.id)
                    .type(Delete.class.getName())
                    .region(this.region)
                    .endpointOverride(this.endpointOverride)
                    .accessKeyId(this.accessKeyId)
                    .secretKeyId(this.secretKeyId)
                    .bucket(request.sourceBucket())
                    .key(request.sourceKey())
                    .build()
                    .run(runContext);
            }

            return Output
                .builder()
                .bucket(request.destinationBucket())
                .key(request.destinationKey())
                .eTag(response.copyObjectResult().eTag())
                .build();
        }
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    public static class CopyObject {
        @Schema(
            title = "The bucket name"
        )
        @PluginProperty(dynamic = true)
        String bucket;

        @Schema(
            title = "The bucket key"
        )
        @PluginProperty(dynamic = true)
        String key;
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    public static class CopyObjectFrom extends CopyObject {
        @Schema(
            title = "The specific version of the object."
        )
        @PluginProperty(dynamic = true)
        String versionId;
    }

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        private final String bucket;
        private final String key;
    }
}

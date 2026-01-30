package io.kestra.plugin.aws.s3;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import io.kestra.plugin.aws.s3.models.S3ServerSideEncryption;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: aws_s3_copy
                namespace: company.team

                tasks:
                  - id: copy
                    type: io.kestra.plugin.aws.s3.Copy
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    from:
                      bucket: "my-bucket"
                      key: "path/to/file"
                    to:
                      bucket: "my-bucket2"
                      key: "path/to/file2"
                """
        )
    }
)
@Schema(
    title = "Copy an object between S3 locations",
    description = "Copies an object within or across buckets. Optionally deletes the source after copy. Supports versionId on source and SSE on destination."
)
public class Copy extends AbstractConnection implements AbstractS3, RunnableTask<Copy.Output> {
    @Schema(
        title = "Source object",
        description = "Bucket/key (and optional versionId) to copy from."
    )
    @PluginProperty
    private CopyObjectFrom from;

    @Schema(
        title = "Destination object",
        description = "Bucket/key to copy to; defaults to source when omitted."
    )
    @PluginProperty
    private CopyObject to;

    @Schema(
        title = "Delete source after copy",
        description = "If true, deletes the source object once copy succeeds."
    )
    @Builder.Default
    private Property<Boolean> delete = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (S3Client client = this.client(runContext)) {
            CopyObjectRequest.Builder builder = CopyObjectRequest.builder()
                .sourceBucket(runContext.render(this.from.bucket).as(String.class).orElseThrow())
                .sourceKey(runContext.render(this.from.key).as(String.class).orElseThrow())
                .destinationBucket(runContext.render(this.to.bucket != null ? this.to.bucket : this.from.bucket).as(String.class).orElseThrow())
                .destinationKey(runContext.render(this.to.key != null ? this.to.key : this.from.key).as(String.class).orElseThrow());

            if (this.from.versionId != null) {
                builder.sourceVersionId(runContext.render(this.from.versionId).as(String.class).orElseThrow());
            }

            if (this.to != null && this.to.serverSideEncryption != null) {
                S3ServerSideEncryption rSse = runContext
                    .render(this.to.serverSideEncryption)
                    .as(S3ServerSideEncryption.class)
                    .orElse(null);

                if (rSse != null && rSse != S3ServerSideEncryption.NONE) {
                    builder.serverSideEncryption(
                        software.amazon.awssdk.services.s3.model.ServerSideEncryption.valueOf(rSse.name())
                    );
                }
            }


            CopyObjectRequest request = builder.build();
            CopyObjectResponse response = client.copyObject(request);

            if (runContext.render(this.delete).as(Boolean.class).orElseThrow()) {
                Delete.builder()
                    .id(this.id)
                    .type(Delete.class.getName())
                    .region(this.region)
                    .endpointOverride(this.endpointOverride)
                    .accessKeyId(this.accessKeyId)
                    .secretKeyId(this.secretKeyId)
                    .sessionToken(this.sessionToken)
                    .stsRoleSessionName(this.stsRoleSessionName)
                    .stsRoleExternalId(this.stsRoleExternalId)
                    .stsRoleSessionDuration(this.stsRoleSessionDuration)
                    .stsRoleArn(this.stsRoleArn)
                    .stsEndpointOverride(this.stsEndpointOverride)
                    .bucket(Property.ofValue(request.sourceBucket()))
                    .key(Property.ofValue(request.sourceKey()))
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
    @NoArgsConstructor
    public static class CopyObject {
        @Schema(
            title = "Bucket"
        )
        @NotNull
        Property<String> bucket;

        @Schema(
            title = "Key"
        )
        @NotNull
        Property<String> key;

        @Schema(
            title = "Server side encryption to apply to the target object.",
            description = "Example: AES256 or AWS_KMS"
        )
        private Property<S3ServerSideEncryption> serverSideEncryption;
        
        @Schema(
            title = "KMS Key ARN or Key ID to use when server side encryption is AWS_KMS"
        )
        private Property<String> kmsKeyId;
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @NoArgsConstructor
    public static class CopyObjectFrom extends CopyObject {
        @Schema(
            title = "The specific version of the object."
        )
        private Property<String> versionId;
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        private String bucket;
        private String key;
    }
}

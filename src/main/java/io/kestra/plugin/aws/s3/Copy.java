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
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

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
    title = "Copy a file between S3 buckets."
)
public class Copy extends AbstractConnection implements AbstractS3, RunnableTask<Copy.Output> {
    @Schema(
        title = "The source bucket and key."
    )
    @PluginProperty
    private CopyObjectFrom from;

    @Schema(
        title = "The destination bucket and key."
    )
    @PluginProperty
    private CopyObject to;

    @Schema(
        title = "Whether to delete the source file after download."
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
                ServerSideEncryption renderedSse = runContext.render(this.to.serverSideEncryption).as(ServerSideEncryption.class).orElse(null);

                if (renderedSse != null) {
                    builder.serverSideEncryption(renderedSse);

                    if (renderedSse == ServerSideEncryption.AWS_KMS && this.to.kmsKeyId != null) {
                        String renderedKmsKeyId = runContext.render(this.to.kmsKeyId).as(String.class).orElse(null);
                        if (renderedKmsKeyId != null && !renderedKmsKeyId.isEmpty()) {
                            builder.ssekmsKeyId(renderedKmsKeyId);
                        }
                    }
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
            title = "The bucket name"
        )
        @NotNull
        Property<String> bucket;

        @Schema(
            title = "The bucket key"
        )
        @NotNull
        Property<String> key;

        @Schema(
            title = "Server side encryption to apply to the target object.",
            description = "Example: AES256 or AWS_KMS"
        )
        @PluginProperty(dynamic = true)
        private Property<ServerSideEncryption> serverSideEncryption;

        @Schema(
            title = "KMS Key ARN or Key ID to use when server side encryption is AWS_KMS"
        )
        @PluginProperty(dynamic = true)
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

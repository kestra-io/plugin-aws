package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.s3.models.S3ServerSideEncryption;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;
import software.amazon.awssdk.transfer.s3.model.CompletedCopy;
import software.amazon.awssdk.services.s3.S3AsyncClient;

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

        try (
            S3AsyncClient s3AsyncClient = this.asyncClient(runContext);
            S3TransferManager transferManager = S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build()) {

            CopyObjectRequest.Builder copyObjectBuilder = CopyObjectRequest.builder()
                .sourceBucket(runContext.render(this.from.bucket).as(String.class).orElseThrow())
                .sourceKey(runContext.render(this.from.key).as(String.class).orElseThrow())
                .destinationBucket(
                    runContext.render(
                        (this.to != null && this.to.bucket != null) ? this.to.bucket : this.from.bucket
                    ).as(String.class).orElseThrow()
                )
                .destinationKey(
                    runContext.render(
                        (this.to != null && this.to.key != null) ? this.to.key : this.from.key
                    ).as(String.class).orElseThrow()
                );

            // Optional version ID
            if (this.from.versionId != null) {
                copyObjectBuilder.sourceVersionId(
                    runContext.render(this.from.versionId).as(String.class).orElseThrow()
                );
            }

            // Server-side encryption
            if (this.to != null && this.to.serverSideEncryption != null) {
                S3ServerSideEncryption sse = runContext
                    .render(this.to.serverSideEncryption)
                    .as(S3ServerSideEncryption.class)
                    .orElse(null);

                if (sse != null && sse != S3ServerSideEncryption.NONE) {
                    copyObjectBuilder.serverSideEncryption(
                        software.amazon.awssdk.services.s3.model.ServerSideEncryption.valueOf(sse.name())
                    );

                    // If using AWS_KMS encryption, set the KMS key ID
                    if (sse == S3ServerSideEncryption.AWS_KMS && this.to.kmsKeyId != null) {
                        copyObjectBuilder.ssekmsKeyId(
                            runContext.render(this.to.kmsKeyId).as(String.class).orElseThrow()
                        );
                    }
                }
            }

            CopyObjectRequest copyObjectRequest = copyObjectBuilder.build();

            // TransferManager copy (parallel & multipart aware)
            CopyRequest copyRequest = CopyRequest.builder()
                .copyObjectRequest(copyObjectRequest)
                .build();

            CompletedCopy completedCopy = transferManager
                .copy(copyRequest)
                .completionFuture()
                .join();

            // Optional delete source
            if (runContext.render(this.delete).as(Boolean.class).orElse(false)) {
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
                    .bucket(Property.ofValue(copyObjectRequest.sourceBucket()))
                    .key(Property.ofValue(copyObjectRequest.sourceKey()))
                    .build()
                    .run(runContext);
            }

            return Output.builder()
                .bucket(copyObjectRequest.destinationBucket())
                .key(copyObjectRequest.destinationKey())
                .eTag(completedCopy.response().copyObjectResult().eTag())
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

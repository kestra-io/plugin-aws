package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
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
        title = "The file to upload."
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Schema(
        title = "The key where to upload the file."
    )
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "A map of metadata to store with the object in S3."
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> metadata;

    @Schema(
        title = "Can be used to specify caching behavior along the request/response chain."
    )
    @PluginProperty(dynamic = true)
    private String cacheControl;

    @Schema(
        title = "A standard MIME type describing the format of the contents."
    )
    @PluginProperty(dynamic = true)
    private String contentType;

    @Schema(
        title = "Specifies what content encodings have been applied to the object.",
        description = "And thus, what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field."
    )
    @PluginProperty(dynamic = true)
    private String contentEncoding;

    @Schema(
        title = "Specifies presentational information for the object."
    )
    @PluginProperty(dynamic = true)
    private String contentDisposition;

    @Schema(
        title = "The language the content is in."
    )
    @PluginProperty(dynamic = true)
    private String contentLanguage;

    @Schema(
        title = "The size of the body in bytes.",
        description = "This parameter is useful when the size of the body cannot be determined automatically."
    )
    @PluginProperty
    private Long contentLength;

    @Schema(
        title = "The date and time after which the object is no longer cacheable."
    )
    @PluginProperty(dynamic = true)
    private String expires;

    @Schema(
        title = "The canned ACL to apply to the object."
    )
    @PluginProperty(dynamic = true)
    private String acl;

    @Schema(
        title = "If you don't specify, S3 Standard is the default storage class. Amazon S3 supports other storage classes."
    )
    @PluginProperty
    private StorageClass storageClass;

    @Schema(
        title = "The server-side encryption algorithm used when storing this object in Amazon S3.",
        description = "For example, AES256, aws:kms, aws:kms:dsse"
    )
    @PluginProperty
    private ServerSideEncryption serverSideEncryption;

    @Schema(
        title = "Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption with server-side encryption using Key Management Service (KMS) keys (SSE-KMS).",
        description = "Setting this header to true causes Amazon S3 to use an S3 Bucket Key for object encryption with SSE-KMS."
    )
    @PluginProperty
    private Boolean bucketKeyEnabled;

    @Schema(
        title = "Indicates the algorithm used to create the checksum for the object when using the SDK."
    )
    @PluginProperty
    private ChecksumAlgorithm checksumAlgorithm;

    @Schema(
        title = "The account ID of the expected bucket owner.",
        description = "If the bucket is owned by a different account, the request fails " +
            "with the HTTP status code `403 Forbidden` (access denied)."
    )
    @PluginProperty(dynamic = true)
    private String expectedBucketOwner;

    @Schema(
        title = "The Object Lock mode that you want to apply to this object."
    )
    @PluginProperty
    private ObjectLockMode objectLockMode;

    @Schema(
        title = "Specifies whether a legal hold will be applied to this object."
    )
    @PluginProperty
    private ObjectLockLegalHoldStatus objectLockLegalHoldStatus;

    @Schema(
        title = "The date and time when you want this object's Object Lock to expire. "
    )
    @PluginProperty(dynamic = true)
    private String objectLockRetainUntilDate;

    @Schema(
        title = "The checksum data integrity check to verify that the data received is the same data that was originally sent.",
        description = "Must be used in pair with `checksumAlgorithm` to defined the expect algorithm of these values"
    )
    @PluginProperty(dynamic = true)
    private String checksum;

    @Schema(
        title = "The tag-set for the object."
    )
    @PluginProperty
    private Map<String, String> tagging;

    @Schema(
        title = "This property will use the AWS S3 DefaultAsyncClient instead of the S3CrtAsyncClient, which maximizes compatibility with S3-compatible services but restricts uploads and downloads to 2GB."
    )
    @PluginProperty
    @Builder.Default
    private Boolean compatibilityMode = false;

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
                builder.metadata(runContext.renderMap(this.metadata));
            }

            if (this.cacheControl != null) {
                builder.cacheControl(runContext.render(this.cacheControl));
            }

            if (this.contentType != null) {
                builder.contentType(runContext.render(this.contentType));
            }

            if (this.contentEncoding != null) {
                builder.contentEncoding(runContext.render(this.contentEncoding));
            }

            if (this.contentDisposition != null) {
                builder.contentDisposition(runContext.render(this.contentDisposition));
            }

            if (this.contentLanguage != null) {
                builder.contentLanguage(runContext.render(this.contentLanguage));
            }

            if (this.contentLength != null) {
                builder.contentLength(this.contentLength);
            }

            if (this.expires != null) {
                builder.expires(Instant.parse(runContext.render(this.expires)));
            }

            if (this.acl != null) {
                builder.acl(runContext.render(this.acl));
            }

            if (this.storageClass != null) {
                builder.storageClass(this.storageClass);
            }

            if (this.serverSideEncryption != null) {
                builder.serverSideEncryption(this.serverSideEncryption);
            }

            if (this.bucketKeyEnabled != null) {
                builder.bucketKeyEnabled(this.bucketKeyEnabled);
            }

            if (this.checksumAlgorithm != null) {
                builder.checksumAlgorithm(this.checksumAlgorithm);
                switch (this.checksumAlgorithm) {
                    case SHA1 -> builder.checksumSHA1(runContext.render(this.checksum));
                    case SHA256 -> builder.checksumSHA256(runContext.render(this.checksum));
                    case CRC32 -> builder.checksumCRC32(runContext.render(this.checksum));
                    case CRC32_C -> builder.checksumCRC32C(runContext.render(this.checksum));
                }
            }

            if (this.expectedBucketOwner != null) {
                builder.expectedBucketOwner(runContext.render(this.expectedBucketOwner));
            }

            if (this.objectLockMode != null) {
                builder.objectLockMode(this.objectLockMode);
            }

            if (this.objectLockLegalHoldStatus != null) {
                builder.objectLockLegalHoldStatus(this.objectLockLegalHoldStatus);
            }

            if (this.objectLockRetainUntilDate != null) {
                builder.objectLockRetainUntilDate(Instant.parse(runContext.render(this.objectLockRetainUntilDate)));
            }

            if (this.tagging != null) {
                builder.tagging(Tagging.builder()
                    .tagSet(runContext.renderMap(this.tagging)
                        .entrySet()
                        .stream()
                        .map(e -> Tag.builder()
                            .key(e.getKey())
                            .value(e.getValue())
                            .build()
                        )
                        .toList()
                    )
                    .build()
                );
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

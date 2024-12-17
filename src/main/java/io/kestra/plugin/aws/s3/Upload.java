package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

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
                id: aws_s3_upload
                namespace: company.team

                inputs:
                  - id: myfile
                    type: FILE

                tasks:
                  - id: upload
                    type: io.kestra.plugin.aws.s3.Upload
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    from: "{{ inputs.myfile }}"
                    bucket: "my-bucket"
                    key: "path/to/file"
                """
        )
    }
)
@Schema(
    title = "Upload a file to a S3 bucket."
)
public class Upload extends AbstractS3Object implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file(s) to upload.",
        description = "Can be a single file, a list of files or json array.",
        anyOf = {List.class, String.class}
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Object from;

    @Schema(
        title = "The key where to upload the file.",
        description = "a full key (with filename) or the directory path if from is multiple files."
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "A map of metadata to store with the object in S3."
    )
    private Property<Map<String, String>> metadata;

    @Schema(
        title = "Can be used to specify caching behavior along the request/response chain."
    )
    private Property<String> cacheControl;

    @Schema(
        title = "A standard MIME type describing the format of the contents."
    )
    private Property<String> contentType;

    @Schema(
        title = "Specifies what content encodings have been applied to the object.",
        description = "And thus, what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field."
    )
    private Property<String> contentEncoding;

    @Schema(
        title = "Specifies presentational information for the object."
    )
    private Property<String> contentDisposition;

    @Schema(
        title = "The language the content is in."
    )
    private Property<String> contentLanguage;

    @Schema(
        title = "The size of the body in bytes.",
        description = "This parameter is useful when the size of the body cannot be determined automatically."
    )
    private Property<Long> contentLength;

    @Schema(
        title = "The date and time after which the object is no longer cacheable."
    )
    private Property<String> expires;

    @Schema(
        title = "The canned ACL to apply to the object."
    )
    private Property<String> acl;

    @Schema(
        title = "If you don't specify, S3 Standard is the default storage class. Amazon S3 supports other storage classes."
    )
    private Property<StorageClass> storageClass;

    @Schema(
        title = "The server-side encryption algorithm used when storing this object in Amazon S3.",
        description = "For example, AES256, aws:kms, aws:kms:dsse"
    )
    private Property<ServerSideEncryption> serverSideEncryption;

    @Schema(
        title = "Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption with server-side encryption using Key Management Service (KMS) keys (SSE-KMS).",
        description = "Setting this header to true causes Amazon S3 to use an S3 Bucket Key for object encryption with SSE-KMS."
    )
    private Property<Boolean> bucketKeyEnabled;

    @Schema(
        title = "Indicates the algorithm used to create the checksum for the object when using the SDK."
    )
    private Property<ChecksumAlgorithm> checksumAlgorithm;

    @Schema(
        title = "The account ID of the expected bucket owner.",
        description = "If the bucket is owned by a different account, the request fails " +
            "with the HTTP status code `403 Forbidden` (access denied)."
    )
    private Property<String> expectedBucketOwner;

    @Schema(
        title = "The Object Lock mode that you want to apply to this object."
    )
    private Property<ObjectLockMode> objectLockMode;

    @Schema(
        title = "Specifies whether a legal hold will be applied to this object."
    )
    private Property<ObjectLockLegalHoldStatus> objectLockLegalHoldStatus;

    @Schema(
        title = "The date and time when you want this object's Object Lock to expire. "
    )
    private Property<String> objectLockRetainUntilDate;

    @Schema(
        title = "The checksum data integrity check to verify that the data received is the same data that was originally sent.",
        description = "Must be used in pair with `checksumAlgorithm` to defined the expect algorithm of these values"
    )
    private Property<String> checksum;

    @Schema(
        title = "The tag-set for the object."
    )
    private Property<Map<String, String>> tagging;

    @Schema(
        title = "This property will use the AWS S3 DefaultAsyncClient instead of the S3CrtAsyncClient, which maximizes compatibility with S3-compatible services but restricts uploads and downloads to 2GB. For some S3 endpoints such as CloudFlare R2, you may need to set this value to `true`."
    )
    @Builder.Default
    private Property<Boolean> compatibilityMode = Property.of(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket).as(String.class).orElseThrow();
        String key = runContext.render(this.key).as(String.class).orElseThrow();

        try (S3AsyncClient client = this.asyncClient(runContext)) {
            PutObjectRequest.Builder builder = PutObjectRequest
                .builder()
                .bucket(bucket)
                .key(key);

            if (this.requestPayer != null) {
                builder.requestPayer(runContext.render(this.requestPayer).as(String.class).orElseThrow());
            }

            if (this.metadata != null) {
                builder.metadata(runContext.render(this.metadata).asMap(String.class, String.class));
            }

            if (this.cacheControl != null) {
                builder.cacheControl(runContext.render(this.cacheControl).as(String.class).orElseThrow());
            }

            if (this.contentType != null) {
                builder.contentType(runContext.render(this.contentType).as(String.class).orElseThrow());
            }

            if (this.contentEncoding != null) {
                builder.contentEncoding(runContext.render(this.contentEncoding).as(String.class).orElseThrow());
            }

            if (this.contentDisposition != null) {
                builder.contentDisposition(runContext.render(this.contentDisposition).as(String.class).orElseThrow());
            }

            if (this.contentLanguage != null) {
                builder.contentLanguage(runContext.render(this.contentLanguage).as(String.class).orElseThrow());
            }

            if (this.contentLength != null) {
                builder.contentLength(runContext.render(this.contentLength).as(Long.class).orElseThrow());
            }

            if (this.expires != null) {
                builder.expires(Instant.parse(runContext.render(this.expires).as(String.class).orElseThrow()));
            }

            if (this.acl != null) {
                builder.acl(runContext.render(this.acl).as(String.class).orElseThrow());
            }

            if (this.storageClass != null) {
                builder.storageClass(runContext.render(this.storageClass).as(StorageClass.class).orElseThrow());
            }

            if (this.serverSideEncryption != null) {
                builder.serverSideEncryption(runContext.render(this.serverSideEncryption).as(ServerSideEncryption.class).orElseThrow());
            }

            if (this.bucketKeyEnabled != null) {
                builder.bucketKeyEnabled(runContext.render(this.bucketKeyEnabled).as(Boolean.class).orElseThrow());
            }

            if (this.checksumAlgorithm != null) {
                var renderedAlgorithm = runContext.render(this.checksumAlgorithm).as(ChecksumAlgorithm.class).orElseThrow();
                var sum = runContext.render(this.checksum).as(String.class).orElse(null);
                builder.checksumAlgorithm(renderedAlgorithm);
                switch (renderedAlgorithm) {
                    case SHA1 -> builder.checksumSHA1(sum);
                    case SHA256 -> builder.checksumSHA256(sum);
                    case CRC32 -> builder.checksumCRC32(sum);
                    case CRC32_C -> builder.checksumCRC32C(sum);
                }
            }

            if (this.expectedBucketOwner != null) {
                builder.expectedBucketOwner(runContext.render(this.expectedBucketOwner).as(String.class).orElseThrow());
            }

            if (this.objectLockMode != null) {
                builder.objectLockMode(runContext.render(this.objectLockMode).as(ObjectLockMode.class).orElseThrow());
            }

            if (this.objectLockLegalHoldStatus != null) {
                builder.objectLockLegalHoldStatus(runContext.render(this.objectLockLegalHoldStatus).as(ObjectLockLegalHoldStatus.class).orElseThrow());
            }

            if (this.objectLockRetainUntilDate != null) {
                builder.objectLockRetainUntilDate(Instant.parse(runContext.render(this.objectLockRetainUntilDate).as(String.class).orElseThrow()));
            }

            if (this.tagging != null) {
                builder.tagging(Tagging.builder()
                    .tagSet(runContext.render(this.tagging).asMap(String.class, String.class)
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

                String[] renderedFroms;
                if (this.from instanceof Collection<?> fromURIs) {
                    renderedFroms = fromURIs.stream().map(throwFunction(from -> runContext.render((String) from))).toArray(String[]::new);
                } else if (this.from instanceof String) {
                    renderedFroms = new String[]{runContext.render((String) this.from)};
                } else {
                    renderedFroms = JacksonMapper.ofJson().readValue(runContext.render((String) this.from), String[].class);
                }

                for (String renderedFrom : renderedFroms) {
                    File tempFile = runContext.workingDir().createTempFile(FilenameUtils.getExtension(renderedFrom)).toFile();
                    URI from = new URI(runContext.render(renderedFrom));
                    Files.copy(runContext.storage().getFile(from), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                    // if multiple files, it's a dir
                    if (renderedFroms.length > 1) {
                        builder.key(Path.of(key, FilenameUtils.getName(renderedFrom)).toString());
                    }

                    PutObjectRequest putObjectRequest = builder.build();

                    FileUpload upload = transferManager.uploadFile(UploadFileRequest.builder()
                        .putObjectRequest(putObjectRequest)
                        .source(tempFile)
                        .build());

                    runContext.logger().debug("Uploading to '{}'", putObjectRequest.key());

                    // wait for the upload
                    PutObjectResponse response = upload.completionFuture().get().response();

                    runContext.metric(Counter.of("file.count", 1));
                    runContext.metric(Counter.of("file.size", tempFile.length()));

                    if (renderedFroms.length == 1) {
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

            return Output
                .builder()
                .bucket(bucket)
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

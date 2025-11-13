package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.s3.models.FileInfo;
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
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
            title = "Upload a FILE input to S3.",
            code = """
                id: aws_s3_upload
                namespace: company.team

                inputs:
                  - id: bucket
                    type: STRING
                    defaults: my-bucket

                  - id: myfile
                    type: FILE

                tasks:
                  - id: upload
                    type: io.kestra.plugin.aws.s3.Upload
                    region: "{{ secret('AWS_DEFAULT_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    from: "{{ inputs.myfile }}"
                    bucket: "{{ inputs.bucket }}"
                    key: "path/to/file"
                """
        ),
        @Example(
            full = true,
            title = "Download a file and upload it to S3.",
            code = """
                    id: upload_file_to_s3
                    namespace: company.team

                    inputs:
                      - id: bucket
                        type: STRING
                        defaults: my-bucket

                      - id: file_url
                        type: STRING
                        defaults: https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip

                    tasks:
                      - id: download_file
                        type: io.kestra.plugin.core.http.Download
                        uri: "{{ inputs.file_url }}"

                      - id: upload_to_s3
                        type: io.kestra.plugin.aws.s3.Upload
                        from: "{{ outputs.download_file.uri }}"
                        key: powerplant/global_power_plant_database.zip
                        bucket: "{{ inputs.bucket }}"
                        region: "{{ secret('AWS_DEFAULT_REGION') }}"
                        accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                        secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    """
        ),
        @Example(
            full = true,
            title = "Upload multiple files to S3 using a JSON array.",
            code = """
                id: upload_multiple_files_from_json_array
                namespace: company.team

                inputs:
                  - id: bucket
                    type: STRING
                    defaults: my-bucket

                tasks:
                  - id: download_file1
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip"

                  - id: download_file2
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://wri-dataportal-prod.s3.amazonaws.com/manual/enhancing-adaptation-ambition-supplementary-materials.zip"

                  - id: upload_files_to_s3
                    type: io.kestra.plugin.aws.s3.Upload
                    from: |
                      [
                        "{{ outputs.download_file1.uri }}",
                        "{{ outputs.download_file2.uri }}"
                      ]
                    key: "path/to/files"
                    bucket: "{{ inputs.bucket }}"
                    region: "{{ secret('AWS_DEFAULT_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                """
        ),
        @Example(
            full = true,
            title = "Upload multiple files to S3 using a Collection.",
            code = """
                id: upload_multiple_files_to_s3
                namespace: company.team

                inputs:
                  - id: bucket
                    type: STRING
                    defaults: my-bucket

                tasks:
                  - id: download_file1
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip"

                  - id: download_file2
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://wri-dataportal-prod.s3.amazonaws.com/manual/enhancing-adaptation-ambition-supplementary-materials.zip"

                  - id: upload_multiple_to_s3
                    type: io.kestra.plugin.aws.s3.Upload
                    from:
                      - "{{ outputs.download_file1.uri }}"
                      - "{{ outputs.download_file2.uri }}"
                    key: "path/to/files"
                    bucket: "{{ inputs.bucket }}"
                    region: "{{ secret('AWS_DEFAULT_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                """
        ),
        @Example(
            full = true,
            title = "Upload multiple files to S3 using a JSON map.",
            code = """
                id: upload_multiple_files_from_json_map
                namespace: company.team

                inputs:
                  - id: bucket
                    type: STRING
                    defaults: my-bucket

                tasks:
                  - id: download_file1
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip"

                  - id: download_file2
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://wri-dataportal-prod.s3.amazonaws.com/manual/enhancing-adaptation-ambition-supplementary-materials.zip"

                  - id: upload_files_to_s3
                    type: io.kestra.plugin.aws.s3.Upload
                    from: |
                      [
                        "first_key": "{{ outputs.download_file1.uri }}",
                        "second_key": "{{ outputs.download_file2.uri }}"
                      ]
                    key: "path/to/files"
                    bucket: "{{ inputs.bucket }}"
                    region: "{{ secret('AWS_DEFAULT_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                """
        ),
        @Example(
            full = true,
            title = "Upload multiple files to S3 using a Map.",
            code = """
                id: upload_multiple_files_to_s3_from_map
                namespace: company.team

                inputs:
                  - id: bucket
                    type: STRING
                    defaults: my-bucket

                tasks:
                  - id: download_file1
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip"

                  - id: download_file2
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://wri-dataportal-prod.s3.amazonaws.com/manual/enhancing-adaptation-ambition-supplementary-materials.zip"

                  - id: upload_multiple_to_s3
                    type: io.kestra.plugin.aws.s3.Upload
                    from:
                      firstKey: "{{ outputs.download_file1.uri }}"
                      secondKey: "{{ outputs.download_file2.uri }}"
                    key: "path/to/files"
                    bucket: "{{ inputs.bucket }}"
                    region: "{{ secret('AWS_DEFAULT_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                """
        )
    },
    metrics = {
        @Metric(
            name = "file.count",
            type = Counter.TYPE,
            unit = "files",
            description = "Total number of files uploaded."
        ),
        @Metric(
            name = "file.size",
            type = Counter.TYPE,
            unit = "bytes",
            description = "Total size in bytes of files uploaded."
        )
    }
)

@Schema(
    title = "Upload a file(s) to a S3 bucket.",
    description = "Uploads a single or multiple files to an Amazon S3 bucket."
)
public class Upload extends AbstractS3Object implements RunnableTask<Upload.Output>, Data.From {
    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION + "\n" +
            "When providing a Map, keys specify the S3 object key (path after bucket prefix), and values are the source file URIs.",
        anyOf = {List.class, String.class, Map.class}
    )
    @NotNull
    private Object from;

    @Schema(
        title = "The key where to upload the file",
        description = "A full key (with filename) or the directory path if from is multiple files"
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
        title = "A standard MIME type that describes the format of the contents."
    )
    private Property<String> contentType;

    @Schema(
        title = "Specifies what content encodings have been applied to the object",
        description = "And thus, what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field."
    )
    private Property<String> contentEncoding;

    @Schema(
        title = "Specifies presentational information for the object"
    )
    private Property<String> contentDisposition;

    @Schema(
        title = "The language the content is in"
    )
    private Property<String> contentLanguage;

    @Schema(
        title = "The size of the body in bytes",
        description = "This parameter is useful when the size of the body cannot be determined automatically."
    )
    private Property<Long> contentLength;

    @Schema(
        title = "The date and time after which the object is no longer cacheable"
    )
    private Property<String> expires;

    @Schema(
        title = "The canned ACL to apply to the object"
    )
    private Property<String> acl;

    @Schema(
        title = "If you don't specify, S3 Standard is the default storage class. Amazon S3 supports other storage classes."
    )
    private Property<StorageClass> storageClass;

    @Schema(
        title = "The server-side encryption algorithm used when storing this object in Amazon S3",
        description = "For example, AES256, aws:kms, aws:kms:dsse"
    )
    private Property<ServerSideEncryption> serverSideEncryption;

    @Schema(
        title = "Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption with server-side encryption using Key Management Service (KMS) keys (SSE-KMS).",
        description = "Setting this header to true causes Amazon S3 to use an S3 Bucket Key for object encryption with SSE-KMS."
    )
    private Property<Boolean> bucketKeyEnabled;

    @Schema(
        title = "Indicates the algorithm used to create the checksum for the object when using the SDK"
    )
    private Property<ChecksumAlgorithm> checksumAlgorithm;

    @Schema(
        title = "The account ID of the expected bucket owner",
        description = "If the bucket is owned by a different account, the request fails " +
            "with the HTTP status code `403 Forbidden` (access denied)."
    )
    private Property<String> expectedBucketOwner;

    @Schema(
        title = "The Object Lock mode that you want to apply to this object"
    )
    private Property<ObjectLockMode> objectLockMode;

    @Schema(
        title = "Specifies whether a legal hold will be applied to this object"
    )
    private Property<ObjectLockLegalHoldStatus> objectLockLegalHoldStatus;

    @Schema(
        title = "The date and time when you want this object's Object Lock to expire"
    )
    private Property<String> objectLockRetainUntilDate;

    @Schema(
        title = "The checksum data integrity check to verify that the data received is the same data that was originally sent.",
        description = "Must be used in pair with `checksumAlgorithm` to defined the expect algorithm of these values"
    )
    private Property<String> checksum;

    @Schema(
        title = "The tag-set for the object"
    )
    private Property<Map<String, String>> tagging;

    @Schema(
        title = "This property will use the AWS S3 DefaultAsyncClient instead of the S3CrtAsyncClient, which maximizes compatibility with S3-compatible services but restricts uploads and downloads to 2GB. For some S3 endpoints such as CloudFlare R2, you may need to set this value to `true`."
    )
    @Builder.Default
    private Property<Boolean> compatibilityMode = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket).as(String.class).orElseThrow();
        String key = runContext.render(this.key).as(String.class).orElseThrow();

        try (S3AsyncClient client = this.asyncClient(runContext);
             S3TransferManager transferManager = S3TransferManager.builder().s3Client(client).build()) {

            Map<String, String> filesToUpload = parseFromProperty(runContext);

            if (filesToUpload.isEmpty()) {
                throw new IllegalArgumentException("No files to upload: the 'from' property contains an empty collection or array");
            }

            return filesToUpload.size() == 1
                ? uploadSingleFile(runContext, transferManager, bucket, key, filesToUpload.values().iterator().next())
                : uploadMultipleFiles(runContext, transferManager, bucket, key, filesToUpload);
        }
    }

    // In case 'from' is defined as list or single element, we construct a map that has file names as keys and file URIs
    // as values where in this case, file names are just the file name part of the file URIs.
    private Map<String, String> uriListToMap(List<String> rUriList) throws Exception {
        Map<String, String> rUriMap = new HashMap<>();
        for (String rUri : rUriList) {
            rUriMap.put(FilenameUtils.getName(rUri), rUri);
        }
        return rUriMap;
    }

    private Map<String, String> parseFromProperty(RunContext runContext) throws Exception {
        if (this.from instanceof String) {
            String rString = runContext.render((String) this.from).trim();
            // Try to parse as JSON (map or list)
            try {
                @SuppressWarnings("unchecked")
                Map<String, String> rMap = JacksonMapper.ofJson().readValue(rString, Map.class);
                return rMap;
            } catch (Exception e) {
                try {
                    @SuppressWarnings("unchecked")
                    List<String> rList = JacksonMapper.ofJson().readValue(rString, List.class);
                    return uriListToMap(rList);
                } catch (Exception ex) {
                    // No valid JSON.
                }
            }
        }

        // Handle Map<String, String> directly (from YAML).
        if (this.from instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> fromMap = (Map<String, Object>) this.from;
            Map<String, String> rMap = new HashMap<>();
            for (Map.Entry<String, Object> entry : fromMap.entrySet()) {
                String rKey = runContext.render(entry.getKey());
                String rValue = runContext.render(entry.getValue().toString());
                rMap.put(rKey, rValue);
            }
            return rMap;
        }

        // Handle Collection or other Data.From compatible types.
        List<String> rArray = Objects.requireNonNull(Data.from(this.from)
            .readAs(runContext, String.class, Object::toString)
            .map(throwFunction(runContext::render))
            .collectList()
            .block());
        return uriListToMap(rArray);
    }

    private Output uploadSingleFile(RunContext runContext, S3TransferManager transferManager,
                                    String bucket, String key, String renderedFrom) throws Exception {
        File tempFile = copyFileToTemp(runContext, renderedFrom);

        PutObjectRequest putObjectRequest = createPutObjectRequest(runContext, bucket, key);

        runContext.logger().debug("Uploading to '{}'", putObjectRequest.key());

        FileUpload upload = transferManager.uploadFile(UploadFileRequest.builder()
            .putObjectRequest(putObjectRequest)
            .source(tempFile)
            .build());

        PutObjectResponse response = upload.completionFuture().get().response();

        recordMetrics(runContext, tempFile);

        return Output.builder()
            .bucket(bucket)
            .key(key)
            .eTag(response.eTag())
            .versionId(response.versionId())
            .build();
    }

    // For the filesToUpload map we always assume relative file keys in the map's keys and Kestra URIs in the values.
    private Output uploadMultipleFiles(RunContext runContext, S3TransferManager transferManager,
                                       String bucket, String baseKey, Map<String, String> filesToUpload) throws Exception {
        Map<String, FileInfo> fileInfoMap = new HashMap<>();

        for (Map.Entry<String, String> entry : filesToUpload.entrySet()) {
            String fileKey = entry.getKey();
            String finalKey = Path.of(baseKey, fileKey).toString();
            String renderedFrom = entry.getValue();

            File tempFile = copyFileToTemp(runContext, renderedFrom);

            PutObjectRequest putObjectRequest = createPutObjectRequest(runContext, bucket, finalKey);

            runContext.logger().debug("Uploading to '{}'", putObjectRequest.key());

            FileUpload upload = transferManager.uploadFile(UploadFileRequest.builder()
                .putObjectRequest(putObjectRequest)
                .source(tempFile)
                .build());

            PutObjectResponse response = upload.completionFuture().get().response();

            FileInfo fileInfo = FileInfo.builder()
                .eTag(response.eTag())
                .versionId(response.versionId())
                .contentLength(tempFile.length())
                .build();

            fileInfoMap.put(fileKey, fileInfo);

            recordMetrics(runContext, tempFile);
        }

        return Output.builder()
            .key(baseKey)
            .bucket(bucket)
            .files(fileInfoMap)
            .build();
    }

    private File copyFileToTemp(RunContext runContext, String renderedFrom) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(FilenameUtils.getExtension(renderedFrom)).toFile();
        URI from = new URI(runContext.render(renderedFrom));
        Files.copy(runContext.storage().getFile(from), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return tempFile;
    }

    private void recordMetrics(RunContext runContext, File file) {
        runContext.metric(Counter.of("file.count", 1));
        runContext.metric(Counter.of("file.size", file.length()));
    }

    private PutObjectRequest createPutObjectRequest(RunContext runContext, String bucket, String key) throws Exception {
        PutObjectRequest.Builder builder = PutObjectRequest.builder()
            .bucket(bucket)
            .key(key);

        applyRequestOptions(runContext, builder);

        return builder.build();
    }

    private void applyRequestOptions(RunContext runContext, PutObjectRequest.Builder builder) throws Exception {
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
    }

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The S3 bucket name",
            description = "The name of the bucket where the file(s) were uploaded"
        )
        private final String bucket;

        @Schema(
            title = "The S3 object key",
            description = "The key (path) where the file(s) were uploaded in the bucket"
        )
        private final String key;

        @Schema(
            title = "Information about uploaded files",
            description = "A map of file names to their corresponding file information. Returned only for multiple file uploads."
        )
        private final Map<String, FileInfo> files;
    }


}

package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.property.URIFetcher;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.models.FileInfo;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import reactor.core.publisher.Flux;
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
import java.util.*;
import java.util.List;
import java.util.function.Function;

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
    title = "Upload files to S3",
    description = "Uploads one or many files to an S3 bucket. Accepts inputs as URIs, lists, or maps. Supports metadata, ACLs, SSE, checksum, and Object Lock options. Compatibility mode enables S3-compatible endpoints but limits size to ~2GB."
)
public class Upload extends AbstractS3Object implements RunnableTask<Upload.Output>, Data.From {
    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION,
        anyOf = {List.class, String.class, Map.class}
    )
    @NotNull
    private Object from;

    @Schema(
        title = "Object key",
        description = "Full key for single upload or base prefix for multi-file uploads."
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "Metadata",
        description = "Key/value metadata stored with the object."
    )
    private Property<Map<String, String>> metadata;

    @Schema(
        title = "Cache-Control"
    )
    private Property<String> cacheControl;

    @Schema(
        title = "Content-Type"
    )
    private Property<String> contentType;

    @Schema(
        title = "Content-Encoding",
        description = "Applied encodings; informs how to decode to the Content-Type."
    )
    private Property<String> contentEncoding;

    @Schema(
        title = "Content-Disposition"
    )
    private Property<String> contentDisposition;

    @Schema(
        title = "Content-Language"
    )
    private Property<String> contentLanguage;

    @Schema(
        title = "Content-Length",
        description = "Explicit length when it cannot be inferred."
    )
    private Property<Long> contentLength;

    @Schema(
        title = "Expires"
    )
    private Property<String> expires;

    @Schema(
        title = "Canned ACL"
    )
    private Property<String> acl;

    @Schema(
        title = "Storage class",
        description = "Defaults to STANDARD if not set."
    )
    private Property<StorageClass> storageClass;

    @Schema(
        title = "Server-side encryption",
        description = "For example AES256, aws:kms, aws:kms:dsse."
    )
    private Property<ServerSideEncryption> serverSideEncryption;

    @Schema(
        title = "Bucket key enabled",
        description = "Use S3 Bucket Key when SSE-KMS is selected."
    )
    private Property<Boolean> bucketKeyEnabled;

    @Schema(
        title = "Checksum algorithm"
    )
    private Property<ChecksumAlgorithm> checksumAlgorithm;

    @Schema(
        title = "Expected bucket owner",
        description = "Reject if the bucket is owned by another account."
    )
    private Property<String> expectedBucketOwner;

    @Schema(
        title = "Object Lock mode"
    )
    private Property<ObjectLockMode> objectLockMode;

    @Schema(
        title = "Legal hold"
    )
    private Property<ObjectLockLegalHoldStatus> objectLockLegalHoldStatus;

    @Schema(
        title = "Retain until date"
    )
    private Property<String> objectLockRetainUntilDate;

    @Schema(
        title = "Checksum value",
        description = "Must match the selected checksumAlgorithm."
    )
    private Property<String> checksum;

    @Schema(
        title = "Tags"
    )
    private Property<Map<String, String>> tagging;

    @Schema(
        title = "Compatibility mode",
        description = "Use default async client for S3-compatible endpoints (limits transfers to ~2GB)."
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
    private Map<String, String> uriListToMap(List<String> rUriList) {
        Map<String, String> rUriMap = new HashMap<>();
        for (String rUri : rUriList) {
            rUriMap.put(FilenameUtils.getName(rUri), rUri);
        }
        return rUriMap;
    }

    private Map<String, String> parseFromProperty(RunContext runContext) throws Exception {

        if (this.from instanceof String fromString && URIFetcher.supports(runContext.render(fromString))) {
            return uriListToMap(List.of(fromString));
        }

        Data data = Data.from(this.from);
        try {
            Function<Map<String, Object>, Map<String, String>> mapper = map -> {
                Map<String, String> result = new HashMap<>();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    result.put(entry.getKey(), entry.getValue().toString());
                }
                return result;
            };

            @SuppressWarnings("unchecked")
            Flux<Map<String, String>> rFromMap = data.readAs(runContext, (Class<Map<String, String>>) Map.of().getClass(), mapper);
            return Objects.requireNonNull(rFromMap.blockFirst());
        } catch (Exception e) {
            runContext.logger().debug("'from' property is not a Map, trying List...", e);
        }

        try {
            @SuppressWarnings("unchecked")
            Flux<List<String>> rFromList = data.readAs(runContext, (Class<List<String>>) List.of().getClass(), map -> map.keySet().stream().toList());
            return uriListToMap(Objects.requireNonNull(rFromList.flatMapIterable(list -> list).collectList().block()));
        } catch (Exception e) {
            runContext.logger().debug("'from' property is not a List, trying String...", e);
        }

        Flux<String> rFromString = data.readAs(runContext, String.class, Object::toString);
        return uriListToMap(Objects.requireNonNull(rFromString.collectList().block()));
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
            title = "Bucket",
            description = "Destination bucket."
        )
        private final String bucket;

        @Schema(
            title = "Key",
            description = "Object key (base for multi-upload)."
        )
        private final String key;

        @Schema(
            title = "Files",
            description = "Per-file upload info (multi-file uploads only)."
        )
        private final Map<String, FileInfo> files;
    }


}

package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.models.FileInfo;
import io.kestra.plugin.aws.s3.models.S3Object;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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
                id: aws_s3_download
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.aws.s3.Download
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    key: "path/to/file"
                """
        )
    },
    metrics = {
        @Metric(
            name = "file.size",
            type = Counter.TYPE,
            unit = "bytes",
            description = "The size of the downloaded file."
        )
    }
)
@Schema(
    title = "Download objects from S3",
    description = "Single-file mode when only key is set; multi-file mode when prefix/delimiter/regexp are provided. Saves downloads to internal storage and returns metadata per object."
)
public class Download extends AbstractS3Object implements RunnableTask<Download.Output> {
    @Schema(
        title = "Object key",
        description = "Key to download in single-file mode."
    )
    private Property<String> key;

    @Schema(
        title = "Version ID",
        description = "Specific version to fetch in single-file mode."
    )
    protected Property<String> versionId;

    @Schema(
        title = "Compatibility mode",
        description = "Use default async client (limits transfers to ~2GB) for S3-compatible endpoints."
    )
    @Builder.Default
    private Property<Boolean> compatibilityMode = Property.ofValue(false);

    @Schema(
        title = "Prefix filter",
        description = "Enables multi-file mode; downloads keys starting with this prefix."
    )
    private Property<String> prefix;

    @Schema(
        title = "Delimiter",
        description = "Groups keys up to the delimiter; enables multi-file mode."
    )
    private Property<String> delimiter;

    @Schema(
        title = "Marker",
        description = "Pagination start key for multi-file mode."
    )
    private Property<String> marker;

    @Schema(
        title = "Max keys",
        description = "Maximum keys per list request in multi-file mode; default 1000."
    )
    @Builder.Default
    private Property<Integer> maxKeys = Property.ofValue(1000);

    @Schema(
        title = "Regexp filter",
        description = "Regex on keys; enables multi-file mode."
    )
    protected Property<String> regexp;

    @Builder.Default
    @Schema(
        title = "Max files",
        description = "Limit returned files in multi-file mode; default 25."
    )
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Schema(
        title = "The account ID of the expected bucket owner",
        description = "Requests will fail with a Forbidden error (access denied) if the bucket is owned by a different account."
    )
    private Property<String> expectedBucketOwner;


    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket).as(String.class).orElseThrow();

        if (isSingleFileMode()) {
            return downloadSingleFile(runContext, bucket);
        } else if (isValidMultipleFilesMode()) {
            return downloadMultipleFiles(runContext, bucket);
        } else {
            throw new IllegalArgumentException("Invalid configuration: either specify 'key' for single file download or at least one filtering parameter (prefix, delimiter, regexp) for multiple files download");
        }
    }

    private boolean isValidMultipleFilesMode() {
        return this.prefix != null || this.delimiter != null || this.regexp != null;
    }

    private boolean isSingleFileMode() {
        return this.key != null &&
            (this.prefix == null && this.delimiter == null && this.regexp == null);
    }

    private Output downloadSingleFile(RunContext runContext, String bucket) throws Exception {
        String key = runContext.render(this.key).as(String.class).orElseThrow();

        try (S3AsyncClient client = this.asyncClient(runContext)) {
            GetObjectRequest request = buildGetObjectRequest(runContext, bucket, key);
            Pair<GetObjectResponse, URI> download = S3Service.download(runContext, client, request);

            return Output.builder()
                .uri(download.getRight())
                .eTag(download.getLeft().eTag())
                .contentLength(download.getLeft().contentLength())
                .contentType(download.getLeft().contentType())
                .metadata(download.getLeft().metadata())
                .versionId(download.getLeft().versionId())
                .build();
        }
    }

    private GetObjectRequest buildGetObjectRequest(RunContext runContext, String bucket, String key) throws Exception {
        GetObjectRequest.Builder builder = GetObjectRequest.builder()
            .bucket(bucket)
            .key(key);

        if (this.versionId != null) {
            builder.versionId(runContext.render(this.versionId).as(String.class).orElseThrow());
        }

        if (this.requestPayer != null) {
            builder.requestPayer(runContext.render(this.requestPayer).as(String.class).orElseThrow());
        }

        if (this.expectedBucketOwner != null) {
            builder.expectedBucketOwner(runContext.render(this.expectedBucketOwner).as(String.class).orElseThrow());
        }

        return builder.build();
    }

    private Output downloadMultipleFiles(RunContext runContext, String bucket) throws Exception {
        List.Output listResult = getObjectsList(runContext);

        if (listResult.getObjects().isEmpty()) {
            runContext.logger().warn("No objects found matching the filter criteria");
        }

        Map<String, FileInfo> files = new HashMap<>();

        try (S3AsyncClient client = this.asyncClient(runContext)) {
            for (S3Object object : listResult.getObjects()) {
                GetObjectRequest request = buildGetObjectRequest(runContext, bucket, object.getKey());
                Pair<GetObjectResponse, URI> download = S3Service.download(runContext, client, request);

                String key = object.getKey();
                files.put(key, FileInfo.builder()
                    .uri(download.getRight())
                    .contentLength(download.getLeft().contentLength())
                    .contentType(download.getLeft().contentType())
                    .metadata(download.getLeft().metadata())
                    .eTag(download.getLeft().eTag())
                    .versionId(download.getLeft().versionId())
                    .build());
            }

            return Output.builder()
                .files(files)
                .build();
        }
    }

    private List.Output getObjectsList(RunContext runContext) throws Exception {
        List listTask = List.builder()
            .id(this.id)
            .type(List.class.getName())
            .region(this.region)
            .endpointOverride(this.endpointOverride)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .sessionToken(this.sessionToken)
            .requestPayer(this.requestPayer)
            .bucket(this.bucket)
            .prefix(this.prefix)
            .delimiter(this.delimiter)
            .marker(this.marker)
            .maxKeys(this.maxKeys)
            .expectedBucketOwner(this.expectedBucketOwner)
            .regexp(this.regexp)
            .filter(Property.ofValue(ListInterface.Filter.FILES))
            .maxFiles(this.maxFiles)
            .stsRoleArn(this.stsRoleArn)
            .stsRoleSessionName(this.stsRoleSessionName)
            .stsRoleExternalId(this.stsRoleExternalId)
            .stsRoleSessionDuration(this.stsRoleSessionDuration)
            .stsEndpointOverride(this.stsEndpointOverride)
            .build();

        return listTask.run(runContext);
    }

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        private final URI uri;

        @Schema(
            title = "Content length (bytes)"
        )
        private final Long contentLength;

        @Schema(
            title = "Content type"
        )
        private final String contentType;

        @Schema(
            title = "Metadata"
        )
        private final Map<String, String> metadata;

        @Schema(
            title = "Files",
            description = "Per-key file info when multi-file mode is used."
        )
        private final Map<String, FileInfo> files;

    }
}

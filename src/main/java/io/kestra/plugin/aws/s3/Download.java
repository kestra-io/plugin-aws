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
    title = "Download a file(s) from an S3 bucket.",
    description = """
        This task can operate in two modes:
        1. Single file mode: when providing only the 'key' parameter, it downloads a specific file from S3.
        2. Multiple files mode: when using filtering parameters (prefix, delimiter, regexp), it downloads multiple files matching the criteria.

        In single file mode, the output contains the properties of a single file (uri, contentLength, etc.).
        In multiple files mode, the output contains maps that associate each file key with its properties (uris, contentLengths, etc.)."""
)
public class Download extends AbstractS3Object implements RunnableTask<Download.Output> {
    @Schema(
        title = "The key of a file to download",
        description = "When specified without filtering options (prefix, delimiter, regexp), the task will download a single file."
    )
    private Property<String> key;

    @Schema(
        title = "The specific version of the object",
        description = "This property is only applicable when downloading a single file with the key parameter."
    )
    protected Property<String> versionId;

    @Schema(
        title = "If set to true, the task will use the AWS S3 DefaultAsyncClient instead of the S3CrtAsyncClient, which better integrates with S3-compatible services but restricts uploads and downloads to 2GB."
    )
    @Builder.Default
    private Property<Boolean> compatibilityMode = Property.ofValue(false);

    @Schema(
        title = "The prefix of files to download",
        description = "When specified, the task switches to multiple files mode and downloads all files with keys starting with this prefix."
    )
    private Property<String> prefix;

    @Schema(
        title = "A character used to group keys",
        description = "When specified, the task switches to multiple files mode. The API returns all keys that share a common prefix up to the delimiter."
    )
    private Property<String> delimiter;

    @Schema(
        title = "Used for pagination in multiple files mode",
        description = "This is the key at which a previous listing ended."
    )
    private Property<String> marker;

    @Schema(
        title = "The maximum number of keys to include in the response in multiple files mode"
    )
    @Builder.Default
    private Property<Integer> maxKeys = Property.ofValue(1000);

    @Schema(
        title = "A regular expression to filter the keys of the objects to download",
        description = "When specified, the task switches to multiple files mode and only downloads files matching the pattern."
    )
    protected Property<String> regexp;

    @Schema(
        title = "The maximum number of files to retrieve at once"
    )
    private Property<Integer> maxFiles;

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
            title = "The size of the body in bytes"
        )
        private final Long contentLength;

        @Schema(
            title = "A standard MIME type describing the format of the object data"
        )
        private final String contentType;

        @Schema(
            title = "A map of metadata to store with the object in S3"
        )
        private final Map<String, String> metadata;

        @Schema(
            title = "Map of object keys to their complete file information (multiple files mode only)"
        )
        private final Map<String, FileInfo> files;

    }
}

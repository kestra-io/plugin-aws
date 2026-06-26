package io.kestra.plugin.aws.s3;

import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonInclude;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.models.S3Object;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import static io.kestra.core.utils.Rethrow.throwFunction;
import io.kestra.core.models.annotations.PluginProperty;

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
                id: aws_s3_downloads
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.aws.s3.Downloads
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                """
        ),
        @Example(
            full = true,
            title = "Download a prefix and verify the stored S3 checksum on each object",
            code = """
                id: aws_s3_downloads_validate_checksum
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.aws.s3.Downloads
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                    validateChecksum: true
                """
        )
    },
    metrics = {
        @Metric(
            name = "files.count",
            type = Counter.TYPE,
            unit = "objects",
            description = "The number of files downloaded from the S3 bucket."
        ),
        @Metric(
            name = "files.size.total",
            type = Counter.TYPE,
            unit = "bytes",
            description = "The total size in bytes of all downloaded files."
        )
    }
)
@Schema(
    title = "Download multiple S3 objects",
    description = "Lists objects with filters, downloads them in bulk, emits metrics, and optionally performs a post-action (move/delete)."
)
public class Downloads extends AbstractS3Object implements RunnableTask<Downloads.Output>, ListInterface, ActionInterface {
    @PluginProperty(group = "source")
    private Property<String> prefix;

    @PluginProperty(group = "processing")
    private Property<String> delimiter;

    @PluginProperty(group = "source")
    private Property<String> marker;

    @PluginProperty(group = "advanced")
    private Property<String> encodingType;

    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Integer> maxKeys = Property.ofValue(1000);

    @Schema(
        title = "Compatibility mode",
        description = "Use default async client for S3-compatible endpoints (limits transfers to ~2GB)."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> compatibilityMode = Property.ofValue(false);

    @PluginProperty(group = "connection")
    private Property<String> expectedBucketOwner;

    @PluginProperty(group = "processing")
    protected Property<String> regexp;

    @Builder.Default
    @PluginProperty(group = "processing")
    protected final Property<Filter> filter = Property.ofValue(Filter.BOTH);

    @Builder.Default
    @Schema(
        title = "Max files",
        description = "Limit returned files; default 25."
    )
    @PluginProperty(group = "processing")
    private Property<Integer> maxFiles = Property.ofValue(25);

    private Property<ActionInterface.Action> action;

    private Copy.CopyObject moveTo;

    @Schema(
        title = "Validate checksum after download",
        description = "When true, requests S3 to return the stored checksum and the AWS SDK verifies the downloaded bytes during transfer. " +
            "Each object must have been uploaded with a checksum algorithm (SHA1, SHA256, CRC32, or CRC32C) for verification to occur; " +
            "if an object has no stored checksum, a warning is logged and the download is not verified."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> validateChecksum = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        List task = List.builder()
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
            .encodingType(this.encodingType)
            .maxKeys(this.maxKeys)
            .expectedBucketOwner(this.expectedBucketOwner)
            .regexp(this.regexp)
            .filter(this.filter)
            .maxFiles(this.maxFiles)
            .stsRoleArn(this.stsRoleArn)
            .stsRoleSessionName(this.stsRoleSessionName)
            .stsRoleExternalId(this.stsRoleExternalId)
            .stsRoleSessionDuration(this.stsRoleSessionDuration)
            .stsEndpointOverride(this.stsEndpointOverride)
            .build();
        List.Output run = task.run(runContext);

        boolean validate = runContext.render(this.validateChecksum).as(Boolean.class).orElse(false);

        try (S3AsyncClient client = this.asyncClient(runContext)) {
            java.util.List<S3Object> list = run
                .getObjects()
                .stream()
                .map(throwFunction(object ->
                {
                    GetObjectRequest.Builder builder = GetObjectRequest.builder()
                        .bucket(runContext.render(bucket).as(String.class).orElseThrow())
                        .key(object.getKey());

                    if (validate) {
                        builder.checksumMode(ChecksumMode.ENABLED);
                    }

                    Pair<GetObjectResponse, URI> download = S3Service.download(runContext, client, builder.build());
                    Pair<String, String> checksum = S3Service.extractChecksum(download.getLeft());

                    return object
                        .withUri(download.getRight())
                        .withChecksumAlgorithm(checksum.getLeft())
                        .withChecksumValue(checksum.getRight());
                }))
                .filter(object -> !object.getKey().endsWith("/")) // filter directory
                .collect(Collectors.toList());

            runContext.metric(Counter.of("files.count", (double) list.size()));

            double totalBytes = 0.0;
            for (S3Object object : list) {
                if (object.getSize() != null) {
                    totalBytes += object.getSize();
                }
            }

            runContext.metric(Counter.of("files.size.total", totalBytes));

            Map<String, URI> outputFiles = list.stream()
                .map(obj -> new AbstractMap.SimpleEntry<>(obj.getKey(), obj.getUri()))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

            S3Service.performAction(
                run.getObjects(),
                this.action,
                this.moveTo,
                runContext,
                this,
                this
            );

            return Output
                .builder()
                .objects(list)
                .outputFiles(outputFiles)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @JsonInclude
        @Schema(
            title = "Objects",
            description = "Downloaded objects with metadata."
        )
        private final java.util.List<S3Object> objects;

        @Schema(
            title = "Output files",
            description = "Map of object key to downloaded URI."
        )
        private final Map<String, URI> outputFiles;
    }
}

package io.kestra.plugin.aws.s3;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.models.S3Object;
import io.kestra.plugin.aws.s3.models.S3ServerSideEncryption;
import io.kestra.plugin.aws.shared.s3.AbstractS3Connection;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Copy all objects under a prefix to another bucket",
            full = true,
            code = """
                id: aws_s3_copy_list
                namespace: company.team

                tasks:
                  - id: copy_prefix
                    type: io.kestra.plugin.aws.s3.CopyList
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    from:
                      bucket: "source-bucket"
                      prefix: "daily-export/"
                    to:
                      bucket: "dest-bucket"
                      prefix: "backup/"
                    delete: false
                """
        ),
        @Example(
            title = "Move objects matching a regex within the same bucket (delete source after copy)",
            full = true,
            code = """
                id: aws_s3_move_list
                namespace: company.team

                tasks:
                  - id: move_logs
                    type: io.kestra.plugin.aws.s3.CopyList
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    from:
                      bucket: "company-logs"
                      prefix: "inbound/"
                      regexp: ".*\\\\.json"
                    to:
                      prefix: "processed/"
                    delete: true
                    concurrent: 4
                """
        )
    },
    metrics = {
        @Metric(
            name = "s3.objects.copied.count",
            type = Counter.TYPE,
            unit = "objects",
            description = "The total number of objects copied."
        ),
        @Metric(
            name = "s3.objects.copied.size",
            type = Counter.TYPE,
            unit = "bytes",
            description = "The total size in bytes of objects copied."
        )
    }
)
@Schema(
    title = "Copy multiple S3 objects",
    description = """
        Lists objects matching prefix/regex filters and copies them server-side to a destination bucket/prefix (optionally in parallel). Supports deleting source objects after successful copy (move operation)."""
)
public class CopyList extends AbstractS3Connection implements RunnableTask<CopyList.Output> {

    @NotNull
    @Schema(title = "Source configuration (bucket, prefix, regex filter)")
    @PluginProperty(group = "source")
    private CopyListSource from;

    @NotNull
    @Schema(title = "Destination configuration (bucket, prefix)")
    @PluginProperty(group = "destination")
    private CopyListDestination to;

    @Schema(
        title = "Delete source objects after copy",
        description = "If true, deletes each source object after it has been successfully copied (move operation)."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> delete = Property.ofValue(false);

    @Min(2)
    @Schema(
        title = "Parallel copies",
        description = "Max concurrent copy operations when set."
    )
    @PluginProperty(group = "execution")
    private Integer concurrent;

    @Schema(
        title = "Error if empty",
        description = "If true, throw when no objects match filters."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> errorOnEmpty = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        var rSourceBucket = runContext.render(this.from.getBucket()).as(String.class).orElseThrow(
            () -> new IllegalArgumentException("The `from.bucket` property is required.")
        );
        var rSourcePrefix = this.from.getPrefix() != null
            ? runContext.render(this.from.getPrefix()).as(String.class).orElse(null)
            : null;

        var rDestBucket = (this.to != null && this.to.getBucket() != null)
            ? runContext.render(this.to.getBucket()).as(String.class).orElse(rSourceBucket)
            : rSourceBucket;

        var rDestPrefix = (this.to != null && this.to.getPrefix() != null)
            ? runContext.render(this.to.getPrefix()).as(String.class).orElse("")
            : "";

        var deleteSource = runContext.render(this.delete).as(Boolean.class).orElse(false);

        S3ServerSideEncryption sse = null;
        String kmsKeyId = null;
        if (this.to != null && this.to.getServerSideEncryption() != null) {
            sse = runContext.render(this.to.getServerSideEncryption()).as(S3ServerSideEncryption.class).orElse(null);
            if (sse == S3ServerSideEncryption.AWS_KMS && this.to.getKmsKeyId() != null) {
                kmsKeyId = runContext.render(this.to.getKmsKeyId()).as(String.class).orElse(null);
            }
        }

        try (S3Client client = this.client(runContext)) {
            List<S3Object> s3Objects = S3Service.list(runContext, client, this.from);

            List<S3Object> filesToCopy = s3Objects.stream()
                .filter(o -> !S3Service.isDirectoryMarker(o))
                .toList();

            if (runContext.render(this.errorOnEmpty).as(Boolean.class).orElse(false) && filesToCopy.isEmpty()) {
                throw new NoSuchElementException(
                    "Unable to find any files to copy on bucket '%s' with prefix='%s'".formatted(rSourceBucket, rSourcePrefix)
                );
            }

            Map<String, S3Object> destinationMap = new HashMap<>(filesToCopy.size());
            List<Pair<S3Object, String>> copyTargets = new ArrayList<>(filesToCopy.size());

            for (S3Object object : filesToCopy) {
                String destKey = S3Service.destinationKey(rDestPrefix, object.getKey(), rSourcePrefix);

                if (rSourceBucket.equals(rDestBucket) && destKey.equals(object.getKey())) {
                    if (deleteSource) {
                        throw new IllegalArgumentException(
                            "Cannot move object '%s': source and destination bucket and key are identical, which would delete the object."
                                .formatted(destKey)
                        );
                    } else {
                        throw new IllegalArgumentException(
                            "Cannot copy object '%s': source and destination bucket and key are identical."
                                .formatted(destKey)
                        );
                    }
                }

                S3Object existing = destinationMap.put(destKey, object);
                if (existing != null) {
                    throw new IllegalStateException(
                        "Cannot copy objects to '%s': multiple source objects would overwrite the same destination key: '%s' and '%s'. Set a `prefix` that covers both folders, or narrow the listing with `prefix` or `regexp`, so each object maps to its own key under destination `prefix`."
                            .formatted(destKey, existing.getKey(), object.getKey())
                    );
                }

                copyTargets.add(Pair.of(object, destKey));
            }

            Function<Pair<S3Object, String>, Pair<S3Object, Long>> copyFunction = copyObject(
                logger, client, rSourceBucket, rDestBucket, deleteSource, sse, kmsKeyId
            );

            Flux<Pair<S3Object, String>> flowable = Flux.fromIterable(copyTargets);
            Flux<Pair<S3Object, Long>> resultFlux;

            if (this.concurrent != null) {
                resultFlux = flowable
                    .parallel(this.concurrent)
                    .runOn(Schedulers.boundedElastic())
                    .map(copyFunction)
                    .sequential();
            } else {
                resultFlux = flowable.map(copyFunction);
            }

            List<Pair<S3Object, Long>> results = Objects.requireNonNullElse(resultFlux.collectList().block(), List.of());

            long totalCount = results.size();
            long totalSize = results.stream().mapToLong(Pair::getRight).sum();
            List<S3Object> copiedObjects = results.stream().map(Pair::getLeft).toList();

            runContext.metric(Counter.of("s3.objects.copied.count", totalCount));
            runContext.metric(Counter.of("s3.objects.copied.size", totalSize));

            logger.info("Copied {} objects ({} bytes) from '{}' to '{}'", totalCount, totalSize, rSourceBucket, rDestBucket);

            return Output.builder()
                .count(totalCount)
                .size(totalSize)
                .objects(copiedObjects)
                .build();
        }
    }

    private static Function<Pair<S3Object, String>, Pair<S3Object, Long>> copyObject(
        Logger logger,
        S3Client client,
        String sourceBucket,
        String destBucket,
        boolean deleteSource,
        S3ServerSideEncryption sse,
        String kmsKeyId
    ) {
        return target -> {
            S3Object object = target.getLeft();
            String destKey = target.getRight();

            logger.debug("Copying '{}/{}' to '{}/{}'", sourceBucket, object.getKey(), destBucket, destKey);

            CopyObjectRequest.Builder copyBuilder = CopyObjectRequest.builder()
                .sourceBucket(sourceBucket)
                .sourceKey(object.getKey())
                .destinationBucket(destBucket)
                .destinationKey(destKey);

            if (sse != null && sse != S3ServerSideEncryption.NONE) {
                copyBuilder.serverSideEncryption(ServerSideEncryption.valueOf(sse.name()));
                if (sse == S3ServerSideEncryption.AWS_KMS && kmsKeyId != null) {
                    copyBuilder.ssekmsKeyId(kmsKeyId);
                }
            }

            CopyObjectResponse response = client.copyObject(copyBuilder.build());
            String etag = response.copyObjectResult() != null ? response.copyObjectResult().eTag() : null;
            Instant lastModified = response.copyObjectResult() != null && response.copyObjectResult().lastModified() != null
                ? response.copyObjectResult().lastModified()
                : object.getLastModified();

            if (deleteSource) {
                logger.debug("Deleting source '{}/{}' after successful copy", sourceBucket, object.getKey());
                client.deleteObject(
                    DeleteObjectRequest.builder()
                        .bucket(sourceBucket)
                        .key(object.getKey())
                        .build()
                );
            }

            S3Object copied = S3Object.builder()
                .key(destKey)
                .etag(etag)
                .size(object.getSize())
                .lastModified(lastModified)
                .owner(object.getOwner())
                .build();

            return Pair.of(copied, object.getSize() != null ? object.getSize() : 0L);
        };
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Builder.Default
        @Schema(title = "The total number of objects copied")
        private final long count = 0;

        @Builder.Default
        @Schema(title = "The total size in bytes of all objects copied")
        private final long size = 0;

        @Schema(title = "List of copied objects output info")
        private final List<S3Object> objects;
    }

    @SuperBuilder
    @ToString
    @EqualsAndHashCode
    @Getter
    @NoArgsConstructor
    public static class CopyListSource implements ListInterface {

        @Schema(
            title = "The S3 bucket to list and copy objects from"
        )
        @NotNull
        @PluginProperty(group = "source")
        private Property<String> bucket;

        @Schema(
            title = "Limits the response to keys that begin with the specified prefix"
        )
        @PluginProperty(group = "source")
        private Property<String> prefix;

        @Schema(
            title = "A delimiter is a character you use to group keys"
        )
        @PluginProperty(group = "processing")
        private Property<String> delimiter;

        @Schema(
            title = "Marker is where you want Amazon S3 to start listing from",
            description = "Amazon S3 starts listing after this specified key."
        )
        @PluginProperty(group = "advanced")
        private Property<String> marker;

        @Schema(
            title = "The EncodingType property for this object"
        )
        @PluginProperty(group = "advanced")
        private Property<String> encodingType;

        @Builder.Default
        @Schema(
            title = "Maximum number of objects returned across all paginated S3 calls",
            description = """
                Total upper bound on the objects returned, counted after `regexp` and `filter`. Default 1000, must be 0 or more, and 0 returns no objects.
                When `regexp` is unset and `filter` is `BOTH`, it also sets the per-page size sent to S3, which AWS caps at 1000.
                The listing pages until this total is reached or the bucket is exhausted, so a `regexp` or `filter` that matches few keys can read every key under `prefix`. Set `prefix` to narrow it."""
        )
        @PluginProperty(group = "advanced")
        private Property<Integer> maxKeys = Property.ofValue(1000);

        @Schema(
            title = "The account ID of the expected bucket owner"
        )
        @PluginProperty(group = "advanced")
        private Property<String> expectedBucketOwner;

        @Schema(
            title = "A regexp to filter on full key"
        )
        @PluginProperty(group = "processing")
        private Property<String> regexp;

        @Builder.Default
        @Schema(
            title = "The type of objects to filter: files, directory, or both"
        )
        @PluginProperty(group = "processing")
        private Property<Filter> filter = Property.ofValue(Filter.BOTH);
    }

    @Builder
    @ToString
    @EqualsAndHashCode
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CopyListDestination {

        @Schema(
            title = "The destination bucket name",
            description = "If omitted, defaults to the source bucket name."
        )
        @PluginProperty(group = "destination")
        private Property<String> bucket;

        @Schema(
            title = "The destination prefix (folder path)"
        )
        @PluginProperty(group = "destination")
        private Property<String> prefix;

        @Schema(
            title = "Server side encryption to apply to the target object",
            description = "Example: AES256 or AWS_KMS"
        )
        @PluginProperty(group = "destination")
        private Property<S3ServerSideEncryption> serverSideEncryption;

        @Schema(
            title = "KMS Key ARN or Key ID to use when server side encryption is AWS_KMS"
        )
        @PluginProperty(group = "destination")
        private Property<String> kmsKeyId;
    }
}

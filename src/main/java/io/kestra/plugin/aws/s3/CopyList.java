package io.kestra.plugin.aws.s3;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
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

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

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
                    bucket: "source-bucket"
                    prefix: "daily-export/"
                    to:
                      bucket: "dest-bucket"
                      prefix: "backup/"
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
        Lists the objects matching `prefix`, `regexp` and `filter`, then copies each one server-side under `to.prefix`, optionally in parallel and deleting the source after the copy (a move).
        A key keeps its path relative to `prefix`, or its full key when `prefix` is not set: with `prefix: a/` and `to.prefix: b/`, `a/x/y.csv` is copied to `b/x/y.csv`.
        Only the first `maxKeys` (default 1000) matching objects are handled per run.
        Server-side copy is limited to objects up to 5 GB, use the `Copy` task for larger objects."""
)
public class CopyList extends AbstractS3Object implements RunnableTask<CopyList.Output>, ListInterface {
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

    @PluginProperty(group = "connection")
    private Property<String> expectedBucketOwner;

    @PluginProperty(group = "processing")
    protected Property<String> regexp;

    @Builder.Default
    @PluginProperty(group = "processing")
    protected final Property<Filter> filter = Property.ofValue(Filter.BOTH);

    @NotNull
    @Valid
    @Schema(title = "Destination bucket and prefix")
    @PluginProperty(group = "destination")
    private Destination to;

    @Schema(
        title = "Delete source objects after copy",
        description = "When true, each source object is deleted once its copy succeeded, which makes the task a move."
    )
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Boolean> delete = Property.ofValue(false);

    @Schema(
        title = "Parallel copies",
        description = "Copies run one at a time when unset, otherwise between 2 and 64 at once."
    )
    @PluginProperty(group = "execution")
    private Property<@Min(2) @Max(64) Integer> concurrent;

    @Schema(
        title = "Error if empty",
        description = "When true, the task fails when no object matches the filters."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> errorOnEmpty = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rSourceBucket = runContext.render(this.bucket).as(String.class).orElseThrow(() -> new IllegalArgumentException("The `bucket` property is required."));
        var rSourcePrefix = runContext.render(this.prefix).as(String.class).orElse(null);
        var rDestBucket = runContext.render(this.to.getBucket()).as(String.class).orElse(rSourceBucket);
        var rDestPrefix = runContext.render(this.to.getPrefix()).as(String.class).orElse("");
        var rDelete = runContext.render(this.delete).as(Boolean.class).orElse(false);
        var rConcurrent = runContext.render(this.concurrent).as(Integer.class).orElse(null);
        var rMaxKeys = runContext.render(this.maxKeys).as(Integer.class).orElse(1000);

        try (var client = this.client(runContext)) {
            var objects = S3Service.list(runContext, client, this, this);

            if (rMaxKeys > 0 && objects.size() >= rMaxKeys) {
                logger.warn(
                    "The listing reached `maxKeys` ({}), so matching objects past it in bucket '{}' may remain. Increase `maxKeys` to handle more in one run.",
                    rMaxKeys, rSourceBucket
                );
            }

            var targets = S3Service.uniqueTargets(
                objects,
                key -> destinationKey(rDestPrefix, key, rSourcePrefix),
                "copy",
                "`to.prefix`"
            );

            if (runContext.render(this.errorOnEmpty).as(Boolean.class).orElse(false) && targets.isEmpty()) {
                throw new NoSuchElementException(
                    "No object to copy in bucket '%s' with prefix '%s'. Check `prefix` and `regexp`.".formatted(rSourceBucket, Objects.toString(rSourcePrefix, ""))
                );
            }

            for (var target : targets) {
                if (rSourceBucket.equals(rDestBucket) && target.getRight().equals(target.getLeft().getKey())) {
                    throw new IllegalArgumentException(
                        "Cannot %s '%s' onto itself: the source and destination bucket and key are identical. Set a different `to.prefix` or `to.bucket`."
                            .formatted(rDelete ? "move" : "copy", target.getRight())
                    );
                }
            }

            var copy = copyObject(runContext, logger, client, rSourceBucket, rDestBucket, rDelete);
            var flux = Flux.fromIterable(targets);
            var sizes = rConcurrent == null
                ? flux.map(copy)
                : flux.parallel(rConcurrent).runOn(Schedulers.boundedElastic()).map(copy).sequential();

            var results = sizes.collectList().block();
            var count = (long) results.size();
            var size = results.stream().mapToLong(Long::longValue).sum();

            runContext.metric(Counter.of("s3.objects.copied.count", count));
            runContext.metric(Counter.of("s3.objects.copied.size", size));
            logger.info("Copied {} objects ({} bytes) from '{}' to '{}'", count, size, rSourceBucket, rDestBucket);

            return Output.builder().count(count).size(size).build();
        }
    }

    static String destinationKey(String destPrefix, String key, String sourcePrefix) {
        var relative = sourcePrefix != null && !sourcePrefix.isEmpty() && key.startsWith(sourcePrefix)
            ? StringUtils.removeStart(key.substring(sourcePrefix.length()), "/")
            : key;
        var base = StringUtils.stripEnd(destPrefix, "/");
        return base.isEmpty() ? relative : "%s/%s".formatted(base, relative);
    }

    private Function<Pair<S3Object, String>, Long> copyObject(
        RunContext runContext,
        Logger logger,
        S3Client client,
        String sourceBucket,
        String destBucket,
        boolean delete
    ) throws Exception {
        var copyRequest = CopyObjectRequest.builder();
        S3Service.applyServerSideEncryption(runContext, copyRequest, this.to.getServerSideEncryption(), this.to.getKmsKeyId());
        var rRequestPayer = runContext.render(this.requestPayer).as(String.class).orElse(null);

        return target -> {
            var sourceKey = target.getLeft().getKey();
            var destKey = target.getRight();
            logger.debug("Copying '{}/{}' to '{}/{}'", sourceBucket, sourceKey, destBucket, destKey);

            try {
                client.copyObject(copyRequest.copy()
                    .sourceBucket(sourceBucket)
                    .sourceKey(sourceKey)
                    .destinationBucket(destBucket)
                    .destinationKey(destKey)
                    .requestPayer(rRequestPayer)
                    .build());
            } catch (RuntimeException e) {
                throw new IllegalStateException(
                    "Failed to copy '%s/%s' to '%s/%s': %s. Check read access on the source and write access on the destination."
                        .formatted(sourceBucket, sourceKey, destBucket, destKey, e.getMessage()),
                    e
                );
            }

            if (delete) {
                try {
                    client.deleteObject(DeleteObjectRequest.builder().bucket(sourceBucket).key(sourceKey).requestPayer(rRequestPayer).build());
                } catch (RuntimeException e) {
                    throw new IllegalStateException(
                        "Copied '%s/%s' to '%s/%s' but could not delete the source: %s. Running the task again is safe."
                            .formatted(sourceBucket, sourceKey, destBucket, destKey, e.getMessage()),
                        e
                    );
                }
            }

            return Objects.requireNonNullElse(target.getLeft().getSize(), 0L);
        };
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The number of objects copied")
        private final long count;

        @Schema(title = "The total size in bytes of the objects copied")
        private final long size;
    }

    @Builder
    @ToString
    @EqualsAndHashCode
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Destination {
        @Schema(
            title = "The destination bucket",
            description = "Defaults to the source bucket."
        )
        @PluginProperty(group = "destination")
        private Property<String> bucket;

        @Schema(title = "The destination prefix")
        @PluginProperty(group = "destination")
        private Property<String> prefix;

        @Schema(title = "Server-side encryption for the copied objects")
        @PluginProperty(group = "destination")
        private Property<S3ServerSideEncryption> serverSideEncryption;

        @Schema(title = "KMS key ARN or ID, when `serverSideEncryption` is `AWS_KMS`")
        @PluginProperty(group = "destination")
        private Property<String> kmsKeyId;
    }
}

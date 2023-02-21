package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.models.S3Object;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.util.NoSuchElementException;
import javax.validation.constraints.Min;

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
                "bucket: \"my-bucket\"",
                "prefix: \"sub-dir\""
            }
        )
    }
)
@Schema(
    title = "Delete a list of key on a S3 bucket."
)
public class DeleteList extends AbstractS3Object implements RunnableTask<DeleteList.Output>, ListInterface {
    private String prefix;

    private String delimiter;

    private String marker;

    private String encodingType;

    @Builder.Default
    private Integer maxKeys = 1000;

    private String expectedBucketOwner;

    protected String regexp;

    @Builder.Default
    protected final Filter filter = Filter.BOTH;

    @Min(2)
    @Schema(
        title = "Number of concurrent parallels deletion"
    )
    @PluginProperty
    private Integer concurrent;

    @Schema(
        title = "raise an error if the file is not found"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final Boolean errorOnEmpty = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String bucket = runContext.render(this.bucket);


        try (S3Client client = this.client(runContext)) {
            Flowable<S3Object> flowable = Flowable
                .create(emitter -> {
                    S3Service
                        .list(runContext, client, this, this)
                            .forEach(emitter::onNext);

                    emitter.onComplete();
                }, BackpressureStrategy.BUFFER);

            Flowable<Long> result;

            if (this.concurrent != null) {
                result = flowable
                    .parallel(this.concurrent)
                    .runOn(Schedulers.io())
                    .map(delete(logger, client, bucket))
                    .sequential();
            } else {
                result = flowable
                    .map(delete(logger, client, bucket));
            }

            Pair<Long, Long> finalResult = result
                .reduce(Pair.of(0L, 0L), (pair, size) -> Pair.of(pair.getLeft() + 1, pair.getRight() + size))
                .blockingGet();

            runContext.metric(Counter.of("count", finalResult.getLeft()));
            runContext.metric(Counter.of("size", finalResult.getRight()));

            if (errorOnEmpty && finalResult.getLeft() == 0) {
                throw new NoSuchElementException("Unable to find any files to delete on " +
                    runContext.render(this.bucket) + " " +
                    "with regexp='" + runContext.render(this.regexp) + "', " +
                    "prefix='" + runContext.render(this.prefix) + "'"
                );
            }

            logger.info("Deleted {} keys for {} bytes", finalResult.getLeft(), finalResult.getValue());

            return Output
                .builder()
                .count(finalResult.getLeft())
                .size(finalResult.getRight())
                .build();
        }
    }

    private static Function<S3Object, Long> delete(Logger logger, S3Client client, String bucket) {
        return o -> {
            logger.debug("Deleting '{}'", o.getKey());

            DeleteObjectRequest.Builder builder = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(o.getKey());

            client.deleteObject(builder.build());


            return o.getSize();
        };
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Builder.Default
        @Schema(
            title = "The count of blobs deleted"
        )
        private final long count = 0;

        @Builder.Default
        @Schema(
            title = "The size of all blobs deleted"
        )
        private final long size = 0;
    }
}

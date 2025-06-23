package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.models.S3Object;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.util.NoSuchElementException;
import java.util.function.Function;

import jakarta.validation.constraints.Min;

import static io.kestra.core.utils.Rethrow.throwConsumer;

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
                id: aws_s3_delete_list
                namespace: company.team

                tasks:
                  - id: delete_list
                    type: io.kestra.plugin.aws.s3.DeleteList
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                """
        )
    }
)
@Schema(
    title = "Delete a list of objects in an S3 bucket."
)
public class DeleteList extends AbstractS3Object implements RunnableTask<DeleteList.Output>, ListInterface {
    private Property<String> prefix;

    private Property<String> delimiter;

    private Property<String> marker;

    private Property<String> encodingType;

    @Builder.Default
    private Property<Integer> maxKeys = Property.of(1000);

    private Property<String> expectedBucketOwner;

    protected Property<String> regexp;

    @Builder.Default
    protected final Property<Filter> filter = Property.of(Filter.BOTH);

    @Min(2)
    @Schema(
        title = "Number of concurrent parallels deletion"
    )
    @PluginProperty
    private Integer concurrent;

    @Schema(
        title = "raise an error if the file is not found"
    )
    @Builder.Default
    private final Property<Boolean> errorOnEmpty = Property.of(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String bucket = runContext.render(this.bucket).as(String.class).orElseThrow();


        try (S3Client client = this.client(runContext)) {
            Flux<S3Object> flowable = Flux
                .create(throwConsumer(emitter -> {
                    S3Service
                        .list(runContext, client, this, this)
                            .forEach(emitter::next);

                    emitter.complete();
                }), FluxSink.OverflowStrategy.BUFFER);

            Flux<Long> result;

            if (this.concurrent != null) {
                result = flowable
                    .parallel(this.concurrent)
                    .runOn(Schedulers.boundedElastic())
                    .map(delete(logger, client, bucket))
                    .sequential();
            } else {
                result = flowable
                    .map(delete(logger, client, bucket));
            }

            Pair<Long, Long> finalResult = result
                .reduce(Pair.of(0L, 0L), (pair, size) -> Pair.of(pair.getLeft() + 1, pair.getRight() + size))
                .block();

            runContext.metric(Counter.of("count", finalResult.getLeft()));
            runContext.metric(Counter.of("size", finalResult.getRight()));

            if (runContext.render(errorOnEmpty).as(Boolean.class).orElseThrow() && finalResult.getLeft() == 0) {
                throw new NoSuchElementException("Unable to find any files to delete on " +
                    runContext.render(this.bucket).as(String.class).orElseThrow() + " " +
                    "with regexp='" + runContext.render(this.regexp).as(String.class).orElse(null) + "', " +
                    "prefix='" + runContext.render(this.prefix).as(String.class).orElse(null) + "'"
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

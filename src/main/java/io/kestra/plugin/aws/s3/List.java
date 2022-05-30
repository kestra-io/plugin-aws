package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.models.S3Object;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.s3.S3Client;

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
    title = "List key on a S3 bucket."
)
public class List extends AbstractS3Object implements RunnableTask<List.Output>, ListInterface {
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

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (S3Client client = this.client(runContext)) {
            java.util.List<S3Object> list = S3Service.list(runContext, client, this, this);

            runContext.metric(Counter.of("size", list.size()));

            runContext.logger().debug(
                "Found '{}' keys on {} with regexp='{}', prefix={}",
                list.size(),
                runContext.render(bucket),
                runContext.render(regexp),
                runContext.render(prefix)
            );

            return Output.builder()
                .objects(list)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of objects"
        )
        private final java.util.List<S3Object> objects;
    }
}

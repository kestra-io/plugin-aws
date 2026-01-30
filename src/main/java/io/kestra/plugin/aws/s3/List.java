package io.kestra.plugin.aws.s3;

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
import software.amazon.awssdk.services.s3.S3Client;

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
                id: aws_s3_list
                namespace: company.team

                tasks:
                  - id: list
                    type: io.kestra.plugin.aws.s3.List
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                """
        )
    },
    metrics = {
        @Metric(
            name = "s3.objects.count",
            type = Counter.TYPE,
            unit = "objects",
            description = "The number of objects returned by the S3 list operation."
        )
    }
)
@Schema(
    title = "List keys of an S3 bucket."
)
public class List extends AbstractS3Object implements RunnableTask<List.Output>, ListInterface {
    private Property<String> prefix;

    private Property<String> delimiter;

    private Property<String> marker;

    private Property<String> encodingType;

    @Builder.Default
    private Property<Integer> maxKeys = Property.ofValue(1000);

    private Property<String> expectedBucketOwner;

    protected Property<String> regexp;

    @Builder.Default
    @Schema(
        title = "The maximum number of files to retrieve at once"
    )
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Builder.Default
    protected final Property<Filter> filter = Property.ofValue(Filter.BOTH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (S3Client client = this.client(runContext)) {
            java.util.List<S3Object> list = S3Service.list(runContext, client, this, this);

            runContext.metric(Counter.of("s3.objects.count", list.size()));

            runContext.logger().debug(
                "Found '{}' keys on {} with regexp='{}', prefix={}",
                list.size(),
                runContext.render(bucket).as(String.class).orElseThrow(),
                runContext.render(regexp).as(String.class).orElse(null),
                runContext.render(prefix).as(String.class).orElse(null)
            );

            int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            if (list.size() > rMaxFiles) {
                runContext.logger().warn(
                    "Listing returned {} files but maxFiles limit is {}. Only the first {} files will be returned. " +
                    "Increase the maxFiles property if you need more files.",
                    list.size(), rMaxFiles, rMaxFiles
                );
                list = list.subList(0, rMaxFiles);
            }

            return Output.builder()
                .objects(list)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @JsonInclude
        @Schema(
            title = "The list of S3 objects."
        )
        private final java.util.List<S3Object> objects;
    }
}

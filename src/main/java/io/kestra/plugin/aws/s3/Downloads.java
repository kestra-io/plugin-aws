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
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

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
    title = "Downloads multiple files from a S3 bucket."
)
public class Downloads extends AbstractS3Object implements RunnableTask<Downloads.Output>, ListInterface, ActionInterface {
    private Property<String> prefix;

    private Property<String> delimiter;

    private Property<String> marker;

    private Property<String> encodingType;

    @Builder.Default
    private Property<Integer> maxKeys = Property.ofValue(1000);

    @Schema(
        title = "This property will use the AWS S3 DefaultAsyncClient instead of the S3CrtAsyncClient, which maximizes compatibility with S3-compatible services but restricts uploads and downloads to 2GB."
    )
    @Builder.Default
    private Property<Boolean> compatibilityMode = Property.ofValue(false);


    private Property<String> expectedBucketOwner;

    protected Property<String> regexp;

    @Builder.Default
    protected final Property<Filter> filter = Property.ofValue(Filter.BOTH);

    private Property<ActionInterface.Action> action;

    private Copy.CopyObject moveTo;

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
            .stsRoleArn(this.stsRoleArn)
            .stsRoleSessionName(this.stsRoleSessionName)
            .stsRoleExternalId(this.stsRoleExternalId)
            .stsRoleSessionDuration(this.stsRoleSessionDuration)
            .stsEndpointOverride(this.stsEndpointOverride)
            .build();
        List.Output run = task.run(runContext);

        try (S3AsyncClient client = this.asyncClient(runContext)) {
            java.util.List<S3Object> list = run
                .getObjects()
                .stream()
                .map(throwFunction(object -> {
                    GetObjectRequest.Builder builder = GetObjectRequest.builder()
                        .bucket(runContext.render(bucket).as(String.class).orElseThrow())
                        .key(object.getKey());

                    Pair<GetObjectResponse, URI> download = S3Service.download(runContext, client, builder.build());

                    return object.withUri(download.getRight());
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
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

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
                .totalSize(totalBytes)
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

        @Schema(
            title = "The downloaded files as a map of from/to URIs."
        )
        private final Map<String, URI> outputFiles;
    }
}
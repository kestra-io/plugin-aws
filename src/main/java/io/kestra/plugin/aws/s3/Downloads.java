package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "Downloads multiple files from a S3 bucket."
)
public class Downloads extends AbstractS3Object implements RunnableTask<List.Output>, ListInterface, ActionInterface {
    private String prefix;

    private String delimiter;

    private String marker;

    private String encodingType;

    @Builder.Default
    private Integer maxKeys = 1000;

    @Schema(
        title = "This property will use the AWS S3 DefaultAsyncClient instead of the S3CrtAsyncClient, which maximizes compatibility with S3-compatible services but restricts uploads and downloads to 2GB."
    )
    @PluginProperty
    @Builder.Default
    private Boolean compatibilityMode = false;


    private String expectedBucketOwner;

    protected String regexp;

    @Builder.Default
    protected final Filter filter = Filter.BOTH;

    private ActionInterface.Action action;

    private Copy.CopyObject moveTo;

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        List task = List.builder()
            .id(this.id)
            .type(List.class.getName())
            .region(this.region)
            .endpointOverride(this.endpointOverride)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
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
            .build();
        List.Output run = task.run(runContext);

        try (S3AsyncClient client = this.asyncClient(runContext)) {
            java.util.List<S3Object> list = run
                .getObjects()
                .stream()
                .map(throwFunction(object -> {
                    GetObjectRequest.Builder builder = GetObjectRequest.builder()
                        .bucket(runContext.render(bucket))
                        .key(object.getKey());

                    Pair<GetObjectResponse, URI> download = S3Service.download(runContext, client, builder.build());

                    return object.withUri(download.getRight());
                }))
                .collect(Collectors.toList());


            S3Service.performAction(
                run.getObjects(),
                this.action,
                this.moveTo,
                runContext,
                this,
                this,
                this
            );

            return List.Output
                .builder()
                .objects(list)
                .build();
        }
    }
}

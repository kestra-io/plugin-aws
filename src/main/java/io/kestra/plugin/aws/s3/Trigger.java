package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.kestra.plugin.aws.s3.models.S3Object;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for files on S3 bucket",
    description = "This trigger will poll every `interval` s3 bucket. " +
        "You can search for all files in a bucket or directory in `from` or you can filter the files with a `regExp`." +
        "The detection is atomic, internally we do a list and interact only with files listed.\n" +
        "Once a file is detected, we download the file on internal storage and processed with declared `action` " +
        "in order to move or delete the files from the bucket (to avoid double detection on new poll)"
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a list of file on a s3 bucket and iterate through the files",
            full = true,
            code = {
                "id: s3-listen",
                "namespace: io.kestra.tests",
                "",
                "tasks:",
                "  - id: each",
                "    type: io.kestra.core.tasks.flows.EachSequential",
                "    tasks:",
                "      - id: return",
                "        type: io.kestra.core.tasks.debugs.Return",
                "        format: \"{{taskrun.value}}\"",
                "    value: \"{{ trigger.objects | jq('.[].uri') }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.aws.s3.Trigger",
                "    accessKeyId: \"<access-key>\"",
                "    secretKeyId: \"<secret-key>\"",
                "    region: \"eu-central-1\"",
                "    interval: \"PT5M\"",
                "    bucket: \"my-bucket\"",
                "    prefix: \"sub-dir\"",
                "    action: MOVE",
                "    moveTo: ",
                "      key: archive",
            }
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<List.Output>, ListInterface, ActionInterface, AbstractS3ObjectInterface, AbstractConnectionInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected String accessKeyId;

    protected String secretKeyId;

    protected String sessionToken;

    protected String region;

    protected String endpointOverride;

    protected String requestPayer;

    protected String bucket;

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

    private ActionInterface.Action action;

    private Copy.CopyObject moveTo;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

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

        if (run.getObjects().size() == 0) {
            return Optional.empty();
        }

        java.util.List<S3Object> list = run
            .getObjects()
            .stream()
            .map(throwFunction(object -> {
                Download download = Download.builder()
                    .id(this.id)
                    .type(List.class.getName())
                    .region(this.region)
                    .endpointOverride(this.endpointOverride)
                    .accessKeyId(this.accessKeyId)
                    .secretKeyId(this.secretKeyId)
                    .requestPayer(this.requestPayer)
                    .bucket(this.bucket)
                    .key(object.getKey())
                    .build();
                Download.Output downloadOutput = download.run(runContext);

                return object.withUri(downloadOutput.getUri());
            }))
            .collect(Collectors.toList());

        S3Service.archive(
            run.getObjects(),
            this.action,
            this.moveTo,
            runContext,
            this,
            this,
            this
        );
        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
            this,
            List.Output.builder().objects(list).build()
        );
        Execution execution = Execution.builder()
            .id(runContext.getTriggerExecutionId())
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .trigger(executionTrigger)
            .build();

        return Optional.of(execution);
    }
}

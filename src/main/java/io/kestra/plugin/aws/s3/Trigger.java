package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
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
    title = "Trigger a flow on a new file arrival in an S3 bucket.",
    description = "This trigger will poll every `interval` s3 bucket. " +
        "You can search for all files in a bucket or directory in `from` or you can filter the files with a `regExp`. " +
        "The detection is atomic, internally we do a list and interact only with files listed.\n" +
        "Once a file is detected, we download the file on internal storage and process with a declared `action` " +
        "in order to move or delete the files from the bucket (to avoid double detection on new poll)."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a list of files on a s3 bucket and iterate through the files.",
            full = true,
            code = """
                id: s3_listen
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.objects | jq('.[].uri') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.aws.s3.Trigger
                    interval: "PT5M"
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                    action: MOVE
                    moveTo:
                      key: archive
                      bucket: "new-bucket"
                """
        ),
        @Example(
            title = "Wait for a list of files on a s3 bucket and iterate through the files. Delete files manually after processing to prevent infinite triggering.",
            full = true,
            code = """
                id: s3_listen
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.objects | jq('.[].key') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value }}"

                      - id: delete
                        type: io.kestra.plugin.aws.s3.Delete
                        accessKeyId: "<access-key>"
                        secretKeyId: "<secret-key>"
                        region: "eu-central-1"
                        bucket: "my-bucket"
                        key: "{{ taskrun.value }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.aws.s3.Trigger
                    interval: "PT5M"
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                    action: NONE
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<List.Output>, ListInterface, ActionInterface, AbstractS3ObjectInterface, AbstractConnectionInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected Property<String> accessKeyId;

    protected Property<String> secretKeyId;

    protected Property<String> sessionToken;

    protected Property<String> region;

    protected Property<String> endpointOverride;

    protected Property<String> requestPayer;

    protected Property<String> bucket;

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

    private Property<ActionInterface.Action> action;

    private Copy.CopyObject moveTo;

    // Configuration for AWS STS AssumeRole
    protected Property<String> stsRoleArn;
    protected Property<String> stsRoleExternalId;
    protected Property<String> stsRoleSessionName;
    protected Property<String> stsEndpointOverride;
    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.of(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

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

        if (run.getObjects().isEmpty()) {
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
                    .sessionToken(this.sessionToken)
                    .stsRoleArn(this.stsRoleArn)
                    .stsRoleSessionName(this.stsRoleSessionName)
                    .stsRoleExternalId(this.stsRoleExternalId)
                    .stsRoleSessionDuration(this.stsRoleSessionDuration)
                    .stsEndpointOverride(this.stsEndpointOverride)
                    .requestPayer(this.requestPayer)
                    .bucket(this.bucket)
                    .key(Property.of(object.getKey()))
                    .build();
                Download.Output downloadOutput = download.run(runContext);

                return object.withUri(downloadOutput.getUri());
            }))
            .collect(Collectors.toList());

        S3Service.performAction(
            run.getObjects(),
            this.action,
            this.moveTo,
            runContext,
            this,
            this
        );

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, List.Output.builder().objects(list).build());

        return Optional.of(execution);
    }
}

package io.kestra.plugin.aws.s3;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
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

import java.sql.Blob;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on S3 object creation/update",
    description = "Polls a bucket on a fixed interval, listing with prefix/regex filters. Downloads matched objects to internal storage and can move or delete them to prevent reprocessing. Maintains state per trigger to avoid duplicates."
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
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
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
                        accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                        secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                        region: "eu-central-1"
                        bucket: "my-bucket"
                        key: "{{ taskrun.value }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.aws.s3.Trigger
                    interval: "PT5M"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                    action: NONE
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, ListInterface, ActionInterface, AbstractS3ObjectInterface, AbstractConnectionInterface, StatefulTriggerInterface {
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
    private Property<Integer> maxKeys = Property.ofValue(1000);

    private Property<String> expectedBucketOwner;

    protected Property<String> regexp;

    @Builder.Default
    protected final Property<Filter> filter = Property.ofValue(Filter.BOTH);

    private Property<ActionInterface.Action> action;

    private Copy.CopyObject moveTo;

    // Configuration for AWS STS AssumeRole
    protected Property<String> stsRoleArn;
    protected Property<String> stsRoleExternalId;
    protected Property<String> stsRoleSessionName;
    protected Property<String> stsEndpointOverride;
    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    @Builder.Default
    private Property<Boolean> compatibilityMode = Property.ofValue(false);

    @Builder.Default
    private Property<Boolean> forcePathStyle = Property.ofValue(false);

    @Builder.Default
    private final Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    @Builder.Default
    @Schema(
        title = "The maximum number of files to retrieve at once"
    )
    private Property<Integer> maxFiles = Property.ofValue(25);

    private Property<String> stateKey;

    private Property<Duration> stateTtl;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey).as(String.class).orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);

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
            .maxFiles(this.maxFiles)
            .stsRoleArn(this.stsRoleArn)
            .stsRoleSessionName(this.stsRoleSessionName)
            .stsRoleExternalId(this.stsRoleExternalId)
            .stsRoleSessionDuration(this.stsRoleSessionDuration)
            .stsEndpointOverride(this.stsEndpointOverride)
            .compatibilityMode(this.compatibilityMode)
            .forcePathStyle(this.forcePathStyle)
            .build();

        List.Output run = task.run(runContext);

        if (run.getObjects().isEmpty()) {
            return Optional.empty();
        }

        var previousState = readState(runContext, rStateKey, rStateTtl);

        java.util.List<S3Object> actionBlobs = new ArrayList<>();

        var toFire = run.getObjects().stream()
            .flatMap(throwFunction(object -> {
                var resolvedBucket = runContext.render(bucket).as(String.class).orElse("");
                var uri = String.format("s3://%s/%s", resolvedBucket, object.getKey());

                Instant modifiedAt = Optional.ofNullable(object.getLastModified()).orElse(Instant.now());
                String version = Optional.ofNullable(object.getEtag()).orElse(String.valueOf(modifiedAt.toEpochMilli()));

                var candidate = StatefulTriggerService.Entry.candidate(uri, version, modifiedAt);
                var change = computeAndUpdateState(previousState, candidate, rOn);

                if (change.fire()) {
                    var changeType = change.isNew() ? ChangeType.CREATE : ChangeType.UPDATE;

                    var download = Download.builder()
                        .id(this.id)
                        .type(Download.class.getName())
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
                        .key(Property.ofValue(object.getKey()))
                        .compatibilityMode(this.compatibilityMode)
                        .forcePathStyle(this.forcePathStyle)
                        .build();

                    var dlOut = download.run(runContext);
                    var downloaded = object.withUri(dlOut.getUri());

                    actionBlobs.add(object);

                    return Stream.of(TriggeredObject.builder()
                        .object(downloaded)
                        .changeType(changeType)
                        .build());
                }
                return Stream.empty();
            }))
            .toList();

        writeState(runContext, rStateKey, previousState, rStateTtl);

        if (toFire.isEmpty()) {
            return Optional.empty();
        }

        S3Service.performAction(actionBlobs, this.action, this.moveTo, runContext, this, this);

        var output = Output.builder().objects(toFire).build();
        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);

        return Optional.of(execution);
    }

    public enum ChangeType {
        CREATE,
        UPDATE
    }

    @Getter
    @AllArgsConstructor
    @Builder
    public static class TriggeredObject {
        @JsonUnwrapped
        private final S3Object object;
        private final ChangeType changeType;

        public S3Object toObject() {
            return object;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of S3 objects that triggered the flow, each with its change type.")
        private final java.util.List<TriggeredObject> objects;
    }
}

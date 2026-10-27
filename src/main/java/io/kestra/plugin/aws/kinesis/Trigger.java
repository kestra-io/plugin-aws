package io.kestra.plugin.aws.kinesis;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.shared.AbstractConnectionInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on Kinesis records (polling)",
    description = "Polls a Kinesis stream on a fixed interval and starts a flow when new records are found. Stores last sequence per shard to avoid reprocessing."
)
@Plugin(
    examples = {
        @Example(
            title = "Poll a Kinesis stream every 30 seconds",
            full = true,
            code = """
                id: kinesis_poll
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "Consumed {{ trigger.count }} records."

                triggers:
                  - id: poll
                    type: io.kestra.plugin.aws.kinesis.Trigger
                    streamName: "stream"
                    iteratorType: "LATEST"
                    interval: PT30S
                    maxRecords: 100
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output> {
    @Schema(
        title = "Access Key Id in order to connect to AWS",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    @PluginProperty(secret = true, group = "advanced")
    private Property<String> accessKeyId;

    @Schema(
        title = "Secret Key Id in order to connect to AWS",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    @PluginProperty(secret = true, group = "advanced")
    private Property<String> secretKeyId;

    @Schema(
        title = "AWS session token, retrieved from an AWS token service, used for authenticating that this user has received temporary permissions to access a given resource",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    @PluginProperty(secret = true, group = "connection")
    private Property<String> sessionToken;

    @Schema(
        title = "AWS region with which the SDK should communicate"
    )
    @PluginProperty(group = "connection")
    private Property<String> region;

    @Schema(
        title = "The endpoint with which the SDK should communicate",
        description = "This property allows you to target a custom or AWS-compatible Kinesis endpoint (for example, a local test endpoint)."
    )
    @PluginProperty(group = "advanced")
    private Property<String> endpointOverride;

    @Schema(
        title = "AWS STS Role",
        description = "The Amazon Resource Name (ARN) of the role to assume. If set the task will use the `StsAssumeRoleCredentialsProvider`. If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> stsRoleArn;

    @Schema(
        title = "AWS STS External Id",
        description = " A unique identifier that might be required when you assume a role in another account. This property is only used when an `stsRoleArn` is defined."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> stsRoleExternalId;

    @Schema(
        title = "AWS STS Session name",
        description = "This property is only used when an `stsRoleArn` is defined."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> stsRoleSessionName;

    @Schema(
        title = "The AWS STS endpoint with which the SDKClient should communicate"
    )
    @PluginProperty(group = "advanced")
    protected Property<String> stsEndpointOverride;

    @Builder.Default
    @Schema(
        title = "AWS STS Session duration",
        description = "The duration of the role session (default: 15 minutes, i.e., PT15M). This property is only used when an `stsRoleArn` is defined."
    )
    @PluginProperty(group = "execution")
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    @Builder.Default
    @Schema(title = "Interval")
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Stream name",
        description = "Name of the Kinesis stream to poll."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> streamName;

    @Builder.Default
    @Schema(
        title = "Iterator type",
        description = "Start position: LATEST, TRIM_HORIZON, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER."
    )
    private Property<AbstractKinesis.IteratorType> iteratorType = Property.ofValue(AbstractKinesis.IteratorType.LATEST);

    @Schema(
        title = "Starting sequence number",
        description = "Required when iteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER."
    )
    @PluginProperty(group = "advanced")
    private Property<String> startingSequenceNumber;

    @Schema(
        title = "State key",
        description = """
            Key under which per-shard sequence positions are persisted.
            Defaults to a length-prefixed key derived from namespace, flow, trigger, region, endpoint and stream.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> stateKey;

    @Schema(
        title = "State TTL",
        description = """
            How long persisted shard positions are retained; unset means no expiry.
            Should exceed the stream retention period plus the longest expected pause of the trigger. If the TTL expires while the trigger is disabled or paused, TRIM_HORIZON will re-deliver all records still within the stream retention period.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;

    @Builder.Default
    @Schema(title = "Max records")
    private Property<Integer> maxRecords = Property.ofValue(1000);

    @Builder.Default
    @Schema(title = "Max duration")
    private Property<Duration> maxDuration = Property.ofValue(Duration.ofSeconds(30));

    @Builder.Default
    @Schema(title = "Poll duration")
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(1));

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        var rStream = runContext.render(streamName).as(String.class).orElseThrow();
        var rRegion = runContext.render(region).as(String.class).orElse("");
        var rEndpoint = runContext.render(endpointOverride).as(String.class).orElse("");
        var streamScope = stateScope(rRegion, rEndpoint, rStream);
        var rStateKey = runContext.render(stateKey).as(String.class).orElse(stateKey(context, rRegion, rEndpoint, rStream));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);
        var previousState = StatefulTriggerService.readState(runContext, rStateKey, rStateTtl);
        var sequences = new HashMap<String, String>();
        previousState.forEach((key, entry) -> {
            if (key.startsWith(streamScope)) {
                sequences.put(key.substring(streamScope.length()), entry.version());
            }
        });

        Consume consume = Consume.builder()
            .streamName(this.streamName)
            .iteratorType(this.iteratorType)
            .startingSequenceNumber(this.startingSequenceNumber)
            .startingSequencePerShard(Property.ofValue(sequences))
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .pollDuration(this.pollDuration)
            .accessKeyId(accessKeyId)
            .secretKeyId(secretKeyId)
            .region(region)
            .sessionToken(sessionToken)
            .endpointOverride(endpointOverride)
            .stsRoleArn(stsRoleArn)
            .stsRoleExternalId(stsRoleExternalId)
            .stsRoleSessionName(stsRoleSessionName)
            .stsEndpointOverride(stsEndpointOverride)
            .stsRoleSessionDuration(stsRoleSessionDuration)
            .build();

        Consume.Output output = consume.run(runContext);

        output.getLastSequencePerShard().forEach((shard, sequence) -> previousState.put(streamScope + shard, StatefulTriggerService.Entry.candidate(streamScope + shard, sequence, Instant.now())));
        StatefulTriggerService.writeState(runContext, rStateKey, previousState, rStateTtl);

        if (logger.isDebugEnabled()) {
            logger.debug("Consumed '{}' messaged.", output.getCount());
        }

        if (output.getCount() == 0) {
            return Optional.empty();
        }

        return Optional.of(
            TriggerService.generateExecution(this, conditionContext, context, output)
        );
    }

    private static String stateKey(TriggerContext context, String region, String endpoint, String stream) {
        return "kinesis_" + lengthPrefixed(context.getNamespace()) + lengthPrefixed(context.getFlowId()) + lengthPrefixed(context.getTriggerId())
            + sha256Hex(region + "\n" + endpoint + "\n" + stream);
    }

    private static String stateScope(String region, String endpoint, String stream) {
        return lengthPrefixed(region) + lengthPrefixed(endpoint) + lengthPrefixed(stream);
    }

    private static String lengthPrefixed(String value) {
        return value.length() + "_" + value;
    }

    private static String sha256Hex(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}

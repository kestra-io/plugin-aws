package io.kestra.plugin.aws.kinesis;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow when new records appear in an Amazon Kinesis stream.",
    description = "A polling trigger that reads new Kinesis records at fixed intervals. "
        + "State is stored per shard using sequence numbers, so each record is processed exactly once."
)
@Plugin(
    examples = {
        @Example(
            title = "Poll a Kinesis stream every 30 seconds",
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
        title = "Access Key Id in order to connect to AWS.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    private Property<String> accessKeyId;

    @Schema(
        title = "Secret Key Id in order to connect to AWS.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    private Property<String> secretKeyId;

    @Schema(
        title = "AWS session token, retrieved from an AWS token service, used for authenticating that this user has received temporary permissions to access a given resource.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    private Property<String> sessionToken;

    @Schema(
        title = "AWS region with which the SDK should communicate."
    )
    private Property<String> region;

    @Schema(
        title = "The endpoint with which the SDK should communicate.",
        description = "This property allows you to use a different S3 compatible storage backend."
    )
    private Property<String> endpointOverride;

    @Schema(
        title = "AWS STS Role.",
        description = "The Amazon Resource Name (ARN) of the role to assume. If set the task will use the `StsAssumeRoleCredentialsProvider`. If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    protected Property<String> stsRoleArn;

    @Schema(
        title = "AWS STS External Id.",
        description = " A unique identifier that might be required when you assume a role in another account. This property is only used when an `stsRoleArn` is defined."
    )
    protected Property<String> stsRoleExternalId;

    @Schema(
        title = "AWS STS Session name.",
        description = "This property is only used when an `stsRoleArn` is defined."
    )
    protected Property<String> stsRoleSessionName;

    @Schema(
        title = "The AWS STS endpoint with which the SDKClient should communicate."
    )
    protected Property<String> stsEndpointOverride;

    @Builder.Default
    @Schema(
        title = "AWS STS Session duration.",
        description = "The duration of the role session (default: 15 minutes, i.e., PT15M). This property is only used when an `stsRoleArn` is defined."
    )
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "The Kinesis stream name.")
    @NotNull
    private Property<String> streamName;

    @Builder.Default
    @Schema(
        title = "The position in the stream to start reading from.",
        description = "Kinesis iterator type: LATEST, TRIM_HORIZON, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER."
    )
    private Property<AbstractKinesis.IteratorType> iteratorType = Property.ofValue(AbstractKinesis.IteratorType.LATEST);

    @Schema(title = "Used if iteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER.")
    private Property<String> startingSequenceNumber;

    @Builder.Default
    private Property<Integer> maxRecords = Property.ofValue(1000);

    @Builder.Default
    private Property<Duration> maxDuration = Property.ofValue(Duration.ofSeconds(30));

    @Builder.Default
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(1));

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Consume consume = Consume.builder()
            .streamName(this.streamName)
            .iteratorType(this.iteratorType)
            .startingSequenceNumber(this.startingSequenceNumber)
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
}

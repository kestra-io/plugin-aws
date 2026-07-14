package io.kestra.plugin.aws.sqs;

import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.kestra.plugin.aws.sqs.model.SerdeType;

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
    title = "Trigger on SQS messages (batch polling)",
    description = "Polls a queue on an interval and creates an execution when messages are fetched, stopping at maxRecords or maxDuration. Messages are stored at trigger.uri; autoDelete controls deletion. For per-message realtime, use RealtimeTrigger."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: sqs
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.data }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.aws.sqs.Trigger
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    queueUrl: "https://sqs.eu-central-1.amazonaws.com/000000000000/test-queue"
                    maxRecords: 10
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, SqsConnectionInterface {

    @Schema(title = "Queue url")
    private Property<String> queueUrl;

    @Schema(title = "Access key id")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    private Property<String> accessKeyId;

    @Schema(title = "Secret key id")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    private Property<String> secretKeyId;

    @Schema(title = "Session token")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    private Property<String> sessionToken;

    @Schema(title = "Region")
    private Property<String> region;

    @Schema(title = "Endpoint override")
    private Property<String> endpointOverride;

    @Schema(
        title = "Maximum concurrent HTTP connections to SQS",
        description = "Connection pool size for the SQS async client. It caps how many requests to SQS " +
            "can be in flight at once. It does not limit how many messages are consumed or how fast the " +
            "queue is drained. Defaults to 50, matching the AWS SDK default. Works together with " +
            "connectionAcquisitionTimeout, which is how long a caller waits for a free connection."
    )
    @Builder.Default
    private Property<Integer> maxConcurrency = Property.ofValue(50);

    @Schema(
        title = "Timeout for acquiring an HTTP connection from the pool",
        description = "How long the SQS async client waits for a free connection before failing. This " +
            "applies when all maxConcurrency connections are already in use. Defaults to 5 seconds."
    )
    @Builder.Default
    private Property<Duration> connectionAcquisitionTimeout = Property.ofValue(Duration.ofSeconds(5));

    @Builder.Default
    @Schema(title = "Interval")
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Max records",
        description = "Stop after consuming this many messages."
    )
    @PluginProperty(group = "execution")
    private Property<Integer> maxRecords;

    @Schema(
        title = "Max duration",
        description = "Stop after this duration elapses."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration;

    @Builder.Default
    @NotNull
    @Schema(
        title = "Serde type",
        description = "Serializer/deserializer used for message bodies."
    )
    @PluginProperty(group = "main")
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    // Configuration for AWS STS AssumeRole
    @Schema(title = "Sts role arn")
    protected Property<String> stsRoleArn;
    @Schema(title = "Sts role external id")
    protected Property<String> stsRoleExternalId;
    @Schema(title = "Sts role session name")
    protected Property<String> stsRoleSessionName;
    @Schema(title = "Sts endpoint override")
    protected Property<String> stsEndpointOverride;
    @Builder.Default

    @Schema(title = "Sts role session duration")
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    @Builder.Default
    @Schema(title = "Auto delete")
    private Property<Boolean> autoDelete = Property.ofValue(true);

    @Builder.Default
    @Schema(title = "Visibility timeout")
    private Property<Integer> visibilityTimeout = Property.ofValue(30);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Consume task = Consume.builder()
            .queueUrl(queueUrl)
            .accessKeyId(accessKeyId)
            .autoDelete(this.autoDelete)
            .secretKeyId(secretKeyId)
            .sessionToken(sessionToken)
            .region(region)
            .endpointOverride(endpointOverride)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .serdeType(this.serdeType)
            .stsRoleArn(this.stsRoleArn)
            .stsRoleSessionName(this.stsRoleSessionName)
            .stsRoleExternalId(this.stsRoleExternalId)
            .stsRoleSessionDuration(this.stsRoleSessionDuration)
            .stsEndpointOverride(this.stsEndpointOverride)
            .visibilityTimeout(this.visibilityTimeout)
            .build();

        Consume.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Consumed '{}' messaged.", run.getCount());
        }

        if (run.getCount() == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }
}

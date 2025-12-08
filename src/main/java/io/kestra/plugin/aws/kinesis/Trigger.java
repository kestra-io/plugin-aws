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
                  - id: handle
                    type: io.kestra.plugin.core.log.Log
                    message: "Consumed {{ trigger.value.recordCount }} records."

                triggers:
                  - id: poll
                    type: io.kestra.plugin.aws.kinesis.KinesisTrigger
                    streamName: "my-stream"
                    iteratorType: "LATEST"
                    interval: PT30S
                    maxRecords: 100
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output> {
    private Property<String> accessKeyId;

    private Property<String> secretKeyId;

    private Property<String> sessionToken;

    private Property<String> region;

    private Property<String> endpointOverride;

    protected Property<String> stsRoleArn;

    protected Property<String> stsRoleExternalId;

    protected Property<String> stsRoleSessionName;

    protected Property<String> stsEndpointOverride;

    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @NotNull
    private Property<String> streamName;

    @Builder.Default
    private Property<String> iteratorType = Property.ofValue("LATEST");

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

        Consume consume = Consume.builder()
            .stream(this.streamName)
            .iteratorType(this.iteratorType)
            .startingSequenceNumber(this.startingSequenceNumber)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .pollDuration(this.pollDuration)
            .accessKeyId(accessKeyId)
            .secretKeyId(secretKeyId)
            .sessionToken(sessionToken)
            .endpointOverride(endpointOverride)
            .stsRoleArn(stsRoleArn)
            .stsRoleExternalId(stsRoleExternalId)
            .stsRoleSessionName(stsRoleSessionName)
            .stsEndpointOverride(stsEndpointOverride)
            .stsRoleSessionDuration(stsRoleSessionDuration)
            .build();

        Consume.Output output = consume.run(runContext);

        if (output.getRecordCount() == 0) {
            return Optional.empty();
        }

        return Optional.of(
            TriggerService.generateExecution(this, conditionContext, context, output)
        );
    }
}

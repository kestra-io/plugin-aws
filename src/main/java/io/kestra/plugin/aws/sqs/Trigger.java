package io.kestra.plugin.aws.sqs;

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
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages periodically from an SQS queue and create one execution per batch.",
    description = "Requires `maxDuration` or `maxRecords`.\nNote that you don't need an extra task to consume the message from the event trigger. The trigger will automatically consume messages and you can retrieve their content in your flow using the `{{ trigger.uri }}` variable. If you would like to consume each message from an SQS queue in real-time and create one execution per message, you can use the [io.kestra.plugin.aws.sqs.RealtimeTrigger](https://kestra.io/plugins/plugin-aws/triggers/io.kestra.plugin.aws.sqs.realtimetrigger) instead."
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
                accessKeyId: "<access-key>"
                secretKeyId: "<secret-key>"
                region: "eu-central-1"
                queueUrl: "https://sqs.eu-central-1.amazonaws.com/000000000000/test-queue"
                maxRecords: 10
            """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, SqsConnectionInterface {

    private String queueUrl;

    private String accessKeyId;

    private String secretKeyId;

    private String sessionToken;

    private Property<String> region;

    private String endpointOverride;

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @PluginProperty
    @Schema(title = "Max number of records, when reached the task will end.")
    private Integer maxRecords;

    @PluginProperty
    @Schema(title = "Max duration in the Duration ISO format, after that the task will end.")
    private Duration maxDuration;

    @Builder.Default
    @PluginProperty
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private SerdeType serdeType = SerdeType.STRING;

    // Configuration for AWS STS AssumeRole
    protected String stsRoleArn;
    protected String stsRoleExternalId;
    protected String stsRoleSessionName;
    protected String stsEndpointOverride;
    @Builder.Default
    protected Duration stsRoleSessionDuration = AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Consume task = Consume.builder()
            .queueUrl(runContext.render(queueUrl))
            .accessKeyId(runContext.render(accessKeyId))
            .secretKeyId(runContext.render(secretKeyId))
            .sessionToken(runContext.render(sessionToken))
            .region(region)
            .endpointOverride(runContext.render(endpointOverride))
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .serdeType(this.serdeType)
            .stsRoleArn(this.stsRoleArn)
            .stsRoleSessionName(this.stsRoleSessionName)
            .stsRoleExternalId(this.stsRoleExternalId)
            .stsRoleSessionDuration(this.stsRoleSessionDuration)
            .stsEndpointOverride(this.stsEndpointOverride)
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

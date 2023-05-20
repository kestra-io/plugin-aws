package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
import io.swagger.v3.oas.annotations.media.Schema;
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
    title = "Wait for messages in a SQS queue"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "queueUrl: \"https://sqs.us-east-2.amazonaws.com/000000000000/test-queue\"",
                "maxRecords: 10"
            }
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, SqsConnectionInterface {

    private String queueUrl;

    private String accessKeyId;

    private String secretKeyId;

    private String sessionToken;

    private String region;

    private String endpointOverride;

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @PluginProperty
    @Schema(title = "Max number of records, when reached the task will end.")
    private Integer maxRecords;

    @PluginProperty
    @Schema(title = "Max duration in the Duration ISO format, after that the task will end.")
    private Duration maxDuration;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Consume task = Consume.builder()
            .queueUrl(runContext.render(queueUrl))
            .accessKeyId(runContext.render(accessKeyId))
            .secretKeyId(runContext.render(secretKeyId))
            .sessionToken(runContext.render(sessionToken))
            .region(runContext.render(region))
            .endpointOverride(runContext.render(endpointOverride))
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .build();

        Consume.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Consumed '{}' messaged.", run.getCount());
        }

        if (run.getCount() == 0) {
            return Optional.empty();
        }

        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
            this,
            run
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

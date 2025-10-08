package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.models.annotations.*;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Trigger a new flow when CloudWatch metrics match a query condition.")
@Plugin(
    examples =
        @Example(
            title = "Trigger when a CloudWatch metric query returns non-empty results",
            full = true,
            code = """
                id: aws_cloudwatch_trigger
                namespace: company.team
                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.series }}"
                    tasks:
                      - id: log
                        type: io.kestra.plugin.core.log.Log
                        message: "Datapoint: {{ json(taskrun.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.aws.cloudwatch.Trigger
                    interval: "PT1M"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "us-east-1"
                    namespace: "AWS/EC2"
                    metricName: "CPUUtilization"
                    statistic: "Average"
                    periodSeconds: 60
                    window: PT5M
                    dimensions:
                      - name: "InstanceId"
                        value: "i-0abcd1234ef567890"
                """
    )
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Query.Output> {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private Property<String> namespace;

    private Property<String> metricName;

    private Property<String> statistic;

    private Property<Integer> periodSeconds;

    private Property<Duration> window;

    private Property<List<Query.DimensionKV>> dimensions;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Query.Output output = Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .namespace(this.namespace)
            .metricName(this.metricName)
            .statistic(this.statistic)
            .periodSeconds(this.periodSeconds)
            .window(this.window)
            .dimensions(this.dimensions)
            .build()
            .run(runContext);

        logger.debug("CloudWatch query returned {} datapoints", output.getCount());

        if (output.getCount() == 0) {
            return Optional.empty();
        }

        return Optional.of(
            TriggerService.generateExecution(this, conditionContext, context, output)
        );
    }
}

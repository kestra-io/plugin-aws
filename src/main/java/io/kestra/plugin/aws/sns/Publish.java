package io.kestra.plugin.aws.sns;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.sns.model.Message;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish a message to an AWS SNS topic."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = """
            Send an SMS message using AWS SNS
            """,
            code = """
                id: aws_sns_publish
                namespace: company.team

                tasks:
                  - id: publish
                    type: io.kestra.plugin.aws.sns.Publish
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "{{ secret('AWS_DEFAULT_REGION') }}"
                    topicArn: "arn:aws:sns:eu-central-1:000000000000:MessageTopic"
                    from:
                    - data: Hello World
                    - data: Hello Kestra
                      subject: Kestra
                """
        ),
        @Example(
            full = true,
            title = """
            Send an SMS message using AWS SNS based on a runtime-specific input
            """,
            code = """
                id: send_sms
                namespace: company.team
                
                inputs:
                  - id: sms_text
                    type: STRING
                    defaults: Hello from Kestra and AWS SNS!
                
                tasks:
                  - id: send_sms
                    type: io.kestra.plugin.aws.sns.Publish
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "{{ secret('AWS_DEFAULT_REGION') }}"
                    topicArn: arn:aws:sns:eu-central-1:123456789:kestra
                    from:
                      data: |
                        {{ inputs.sms_text }}
                """
        )
    },
    metrics = {
        @Metric(
            name = "sns.publish.messages",
            type = Counter.TYPE,
            unit = "messages",
            description = "Number of messages published to the SNS topic."
        )
    }
)
public class Publish extends AbstractSns implements RunnableTask<Publish.Output>,Data.From {
    @NotNull
    @Schema(
        title = "The source of the published data.",
        description = "Can be an internal storage URI, a list of SNS messages, or a single SNS message."
    )
    private Object from;

    @SuppressWarnings("unchecked")
    @Override
    public Publish.Output run(RunContext runContext) throws Exception {
        var topicArn = runContext.render(getTopicArn()).as(String.class).orElseThrow();
        try (var snsClient = this.client(runContext)) {
            Integer count = Data.from(from).read(runContext)
            .map(throwFunction(message -> {
                snsClient.publish(PublishRequest.builder()
                    .topicArn(topicArn)
                    .message(message.getData())
                    .build());
                return 1;
            }))
            .reduce(Integer::sum)
            .blockOptional()
            .orElse(0);

            // metrics
            runContext.metric(Counter.of("sns.publish.messages", count, "topic", topicArn));

            return Output.builder()
                .messagesCount(count)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of published messages.")
        private final Integer messagesCount;
    }
}

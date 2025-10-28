package io.kestra.plugin.aws.sqs;

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
import io.kestra.plugin.aws.sqs.model.Message;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Map;

import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish a message to an AWS SQS queue."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Publish a message to an SQS queue",
            code = """
                id: aws_sqs_publish
                namespace: company.team

                tasks:
                  - id: publish
                    type: io.kestra.plugin.aws.sqs.Publish
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    region: "{{ secret('AWS_DEFAULT_REGION') }}"
                    queueUrl: "https://sqs.eu-central-1.amazonaws.com/000000000000/test-queue"
                    from:
                    - data: Hello World
                    - data: Hello Kestra
                      delaySeconds: 5
                """
        ),
        @Example(
            full = true,
            title = "Publish an input to an SQS queue",
            code = """
                id: sqs_publish_message
                namespace: company.team
                
                inputs:
                  - id: message
                    type: STRING
                    defaults: Hi from Kestra!
                
                tasks:
                  - id: publish_message
                    type: io.kestra.plugin.aws.sqs.Publish
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    region: "{{ secret('AWS_DEFAULT_REGION') }}"
                    queueUrl: https://sqs.eu-central-1.amazonaws.com/123456789/kestra
                    from:
                      data: "{{ inputs.message }}"
                """
        )
    },
    metrics = {
        @Metric(
            name = "sqs.publish.messages",
            type = Counter.TYPE,
            unit = "messages",
            description = "Number of messages published to the SQS queue."
        )
    }
)
public class Publish extends AbstractSqs implements RunnableTask<Publish.Output>,Data.From {
    @NotNull
    @Schema(
        title = "The source of the published data.",
        description = "Can be an internal storage URI, a list of SQS messages, or a single SQS message.",
        anyOf = {String.class, List.class, Message.class}
    )
    private Object from;

    @SuppressWarnings("unchecked")
    @Override
    public Output run(RunContext runContext) throws Exception {
        var queueUrl = runContext.render(getQueueUrl()).as(String.class).orElseThrow();
        try (var sqsClient = this.client(runContext)) {
            Integer count = Data.from(from).read(runContext)
                .map(throwFunction(message -> {
                    sqsClient.sendMessage(SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(message.get("data").toString())
                        .build());
                    return 1;
                }))
                .reduce(Integer::sum)
                .blockOptional()
                .orElse(0);
            
            // metrics
            runContext.metric(Counter.of("sqs.publish.messages", count, "queue", queueUrl));

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

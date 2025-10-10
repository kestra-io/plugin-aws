package io.kestra.plugin.aws.sqs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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
public class Publish extends AbstractSqs implements RunnableTask<Publish.Output> {
    @PluginProperty(dynamic = true)
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
            Integer count;
            Flux<Message> flowable;
            Flux<Integer> resultFlowable;

            if (this.from instanceof String) {
                URI from = new URI(runContext.render((String) this.from));
                if (!from.getScheme().equals("kestra")) {
                    throw new Exception("Invalid from parameter, must be a Kestra internal storage URI");
                }


                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)))) {
                    flowable = FileSerde.readAll(inputStream, Message.class);
                    resultFlowable = this.buildFlowable(flowable, sqsClient, queueUrl, runContext);

                    count = resultFlowable.reduce(Integer::sum).blockOptional().orElse(0);
                }

            } else if (this.from instanceof List) {
                flowable = Flux
                    .fromIterable((List<?>) this.from)
                    .map(map -> JacksonMapper.toMap(map, Message.class));

                resultFlowable = this.buildFlowable(flowable, sqsClient, queueUrl, runContext);

                count = resultFlowable.reduce(Integer::sum).blockOptional().orElse(0);
            } else {
                var msg = JacksonMapper.toMap(this.from, Message.class);
                sqsClient.sendMessage(msg.to(SendMessageRequest.builder().queueUrl(queueUrl), runContext));

                count = 1;
            }

            // metrics
            runContext.metric(Counter.of("sqs.publish.messages", count, "queue", queueUrl));

            return Output.builder()
                .messagesCount(count)
                .build();
        }
    }

    private Flux<Integer> buildFlowable(Flux<Message> flowable, SqsClient sqsClient, String queueUrl, RunContext runContext) throws IllegalVariableEvaluationException {
        return flowable
            .map(throwFunction(message -> {
                sqsClient.sendMessage(message.to(SendMessageRequest.builder().queueUrl(queueUrl), runContext));
                return 1;
            }));
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of published messages.")
        private final Integer messagesCount;
    }
}

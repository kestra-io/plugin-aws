package io.kestra.plugin.aws.sqs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
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
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish a message to an SQS queue."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "queueUrl: \"https://sqs.eu-central-1.amazonaws.com/000000000000/test-queue\"",
                "from:",
                "- data: Hello World",
                "- data: Hello Kestra",
                "  delaySeconds: 5"
            }
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
        var queueUrl = runContext.render(getQueueUrl());
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
                    flowable = Flux.create(FileSerde.reader(inputStream, Message.class), FluxSink.OverflowStrategy.BUFFER);
                    resultFlowable = this.buildFlowable(flowable, sqsClient, queueUrl, runContext);

                    count = resultFlowable.reduce(Integer::sum).block();
                }

            } else if (this.from instanceof List) {
                flowable = Flux
                    .fromArray(((List<Message>) this.from).toArray())
                    .cast(Message.class);

                resultFlowable = this.buildFlowable(flowable, sqsClient, queueUrl, runContext);

                count = resultFlowable.reduce(Integer::sum).block();
            } else {
                var msg = JacksonMapper.toMap(this.from, Message.class);
                sqsClient.sendMessage(msg.to(SendMessageRequest.builder().queueUrl(queueUrl), runContext));

                count = 1;
            }

            // metrics
            runContext.metric(Counter.of("records", count, "queue", queueUrl));

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

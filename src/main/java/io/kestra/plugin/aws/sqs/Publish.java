package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.sqs.model.Message;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish a message to an SQS queue"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "queueUrl: \"https://sqs.us-east-2.amazonaws.com/000000000000/test-queue\"",
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
        description = "Can be an internal storage URI, a list of SQS messages or a single SQS message."
    )
    private Object from;

    @SuppressWarnings("unchecked")
    @Override
    public Output run(RunContext runContext) throws Exception {
        try (var sqsClient = this.client(runContext)) {
            Integer count;
            Flowable<Message> flowable;
            Flowable<Integer> resultFlowable;

            if (this.from instanceof String) {
                URI from = new URI(runContext.render((String) this.from));
                if (!from.getScheme().equals("kestra")) {
                    throw new Exception("Invalid from parameter, must be a Kestra internal storage URI");
                }

                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                    flowable = Flowable.create(FileSerde.reader(inputStream, Message.class), BackpressureStrategy.BUFFER);
                    resultFlowable = this.buildFlowable(flowable, sqsClient);

                    count = resultFlowable.reduce(Integer::sum).blockingGet();
                }

            } else if (this.from instanceof List) {
                flowable = Flowable
                    .fromArray(((List<Message>) this.from).toArray())
                    .cast(Message.class);

                resultFlowable = this.buildFlowable(flowable, sqsClient);

                count = resultFlowable.reduce(Integer::sum).blockingGet();
            } else {
                var msg = JacksonMapper.toMap(this.from, Message.class);
                sqsClient.sendMessage(msg.to(SendMessageRequest.builder().queueUrl(getQueueUrl())));

                count = 1;
            }

            // metrics
            runContext.metric(Counter.of("records", count, "queue", runContext.render(this.getQueueUrl())));

            return Output.builder()
                .messagesCount(count)
                .build();
        }
    }

    private Flowable<Integer> buildFlowable(Flowable<Message> flowable, SqsClient sqsClient) {
        return flowable
            .map(message -> {
                sqsClient.sendMessage(message.to(SendMessageRequest.builder().queueUrl(getQueueUrl())));
                return 1;
            });
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of published messages.")
        private final Integer messagesCount;
    }
}

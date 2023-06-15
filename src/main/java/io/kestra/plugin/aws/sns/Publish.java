package io.kestra.plugin.aws.sns;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.sns.model.Message;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

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
    title = "Publish a message to a SNS topic"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "topicArn: \"arn:aws:sns:us-east-1:000000000000:MessageTopic\"",
                "from:",
                "- data: Hello World",
                "- data: Hello Kestra",
                "  subject: Kestra"
            }
        )
    }
)
public class Publish extends AbstractSns implements RunnableTask<Publish.Output> {
    @PluginProperty(dynamic = true)
    @NotNull
    @Schema(
        title = "The source of the published data.",
        description = "Can be an internal storage URI, a list of SNS messages or a single SNS message."
    )
    private Object from;

    @SuppressWarnings("unchecked")
    @Override
    public Publish.Output run(RunContext runContext) throws Exception {
        var topicArn = runContext.render(getTopicArn());
        try (var snsClient = this.client(runContext)) {
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
                    resultFlowable = this.buildFlowable(flowable, snsClient, topicArn, runContext);

                    count = resultFlowable.reduce(Integer::sum).blockingGet();
                }

            } else if (this.from instanceof List) {
                flowable = Flowable
                    .fromArray(((List<Message>) this.from).toArray())
                    .cast(Message.class);

                resultFlowable = this.buildFlowable(flowable, snsClient, topicArn, runContext);

                count = resultFlowable.reduce(Integer::sum).blockingGet();
            } else {
                var msg = JacksonMapper.toMap(this.from, Message.class);
                snsClient.publish(msg.to(PublishRequest.builder().topicArn(topicArn), runContext));

                count = 1;
            }

            // metrics
            runContext.metric(Counter.of("records", count, "topic", topicArn));

            return Output.builder()
                .messagesCount(count)
                .build();
        }
    }

    private Flowable<Integer> buildFlowable(Flowable<Message> flowable, SnsClient snsClient, String topicArn, RunContext runContext) {
        return flowable
            .map(message -> {
                snsClient.publish(message.to(PublishRequest.builder().topicArn(topicArn), runContext));
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

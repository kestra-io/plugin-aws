package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from a SQS queue.",
    description = "Required a maxDuration or a maxRecords."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "queueUrl: \"https://sqs.us-east-2.amazonaws.com/000000000000/test-queue\""
            }
        )
    }
)
public class Consume extends AbstractSqs implements RunnableTask<Consume.Output> {

    @PluginProperty
    @Schema(title = "Max number of records, when reached the task will end.")
    private Integer maxRecords;

    @PluginProperty
    @Schema(title = "Max duration in the Duration ISO format, after that the task will end.")
    private Duration maxDuration;


    @SuppressWarnings("BusyWait")
    @Override
    public Output run(RunContext runContext) throws Exception {
        if (this.maxDuration == null && this.maxRecords == null) {
            throw new IllegalArgumentException("'maxDuration' or 'maxRecords' must be set to avoid an infinite loop");
        }

        try (var sqsClient = this.client(runContext)) {
            var total = new AtomicInteger();
            var started = ZonedDateTime.now();
            var tempFile = runContext.tempFile(".ion").toFile();

            try (var outputFile = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                while (!this.ended(total, started)) {
                    // TODO if we have a maxNumber we can pass the number to avoid too many network calls
                    var receiveRequest = ReceiveMessageRequest.builder().queueUrl(getQueueUrl()).build();
                    var msg = sqsClient.receiveMessage(receiveRequest);
                    msg.messages().forEach(throwConsumer(m -> {
                        FileSerde.write(outputFile, m.body());
                        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(getQueueUrl())
                            .receiptHandle(m.receiptHandle()).build()
                        );
                        total.getAndIncrement();
                    }));

                    Thread.sleep(100);
                }

                runContext.metric(Counter.of("records", total.get(), "queue", runContext.render(this.getQueueUrl())));
                outputFile.flush();
            }

            return Output.builder()
                .uri(runContext.putTempFile(tempFile))
                .count(total.get())
                .build();
        }
    }

    private boolean ended(AtomicInteger count, ZonedDateTime start) {
        if (this.maxRecords != null && count.get() >= this.maxRecords) {
            return true;
        }

        if (this.maxDuration != null && ZonedDateTime.now().toEpochSecond() > start.plus(this.maxDuration).toEpochSecond()) {
            return true;
        }

        return false;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of consumed rows."
        )
        private final Integer count;
        @Schema(
            title = "File URI containing consumed messages."
        )
        private final URI uri;
    }
}

package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.aws.sqs.model.SerdeType;
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

import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from a SQS queue.",
    description = "Requires `maxDuration` or `maxRecords`."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "queueUrl: \"https://sqs.eu-central-1.amazonaws.com/000000000000/test-queue\""
            }
        )
    }
)
public class Consume extends AbstractSqs implements RunnableTask<Consume.Output> {

    @PluginProperty
    @Schema(title = "Maximum number of records; when reached, the task will end.")
    private Integer maxRecords;

    @PluginProperty
    @Schema(title = "Maximum duration in the Duration ISO format, after that the task will end.")
    private Duration maxDuration;

    @Builder.Default
    @PluginProperty
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private SerdeType serdeType = SerdeType.STRING;


    @SuppressWarnings("BusyWait")
    @Override
    public Output run(RunContext runContext) throws Exception {
        var queueUrl = runContext.render(getQueueUrl());
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
                    var receiveRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl).build();
                    var msg = sqsClient.receiveMessage(receiveRequest);
                    msg.messages().forEach(throwConsumer(m -> {
                        FileSerde.write(outputFile, serdeType.deserialize(m.body()));
                        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(m.receiptHandle()).build()
                        );
                        total.getAndIncrement();
                    }));

                    Thread.sleep(100);
                }

                runContext.metric(Counter.of("records", total.get(), "queue", queueUrl));
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

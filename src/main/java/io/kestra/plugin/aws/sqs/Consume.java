package io.kestra.plugin.aws.sqs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.aws.sqs.model.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
    title = "Consume messages from an AWS SQS queue.",
    description = "Requires `maxDuration` or `maxRecords`."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: aws_sqs_consume
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.aws.sqs.Consume
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    queueUrl: "https://sqs.eu-central-1.amazonaws.com/000000000000/test-queue"
                """
        )
    }
)
public class Consume extends AbstractSqs implements RunnableTask<Consume.Output> {

    @Schema(title = "Maximum number of records; when reached, the task will end.")
    private Property<Integer> maxRecords;

    @Schema(title = "Maximum duration in the Duration ISO format, after that the task will end.")
    private Property<Duration> maxDuration;

    @Builder.Default
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @SuppressWarnings("BusyWait")
    @Override
    public Output run(RunContext runContext) throws Exception {
        var queueUrl = runContext.render(getQueueUrl()).as(String.class).orElseThrow();
        if (this.maxDuration == null && this.maxRecords == null) {
            throw new IllegalArgumentException("'maxDuration' or 'maxRecords' must be set to avoid an infinite loop");
        }

        try (var sqsClient = this.client(runContext)) {
            var total = new AtomicInteger();
            var started = ZonedDateTime.now();
            var tempFile = runContext.workingDir().createTempFile(".ion").toFile();

            try (var outputFile = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                do {
                    var receiveRequest = ReceiveMessageRequest.builder()
                        .waitTimeSeconds(1) // this would avoid generating too many calls if there are no messages
                        .queueUrl(queueUrl)
                        .build();
                    var msg = sqsClient.receiveMessage(receiveRequest);
                    msg.messages().forEach(throwConsumer(m -> {
                        FileSerde.write(outputFile, runContext.render(serdeType).as(SerdeType.class).orElseThrow().deserialize(m.body()));
                        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(m.receiptHandle()).build()
                        );
                        total.getAndIncrement();
                    }));

                    Thread.sleep(100);
                } while (!this.ended(total, started, runContext));

                runContext.metric(Counter.of("records", total.get(), "queue", queueUrl));
                outputFile.flush();
            }

            return Output.builder()
                .uri(runContext.storage().putFile(tempFile))
                .count(total.get())
                .build();
        }
    }

    private boolean ended(AtomicInteger count, ZonedDateTime start, RunContext runContext) throws IllegalVariableEvaluationException {
        var max = runContext.render(this.maxRecords).as(Integer.class);
        if (max.isPresent() && count.get() >= max.get()) {
            return true;
        }

        var duration = runContext.render(this.maxDuration).as(Duration.class);
        if (duration.isPresent() && ZonedDateTime.now().toEpochSecond() > start.plus(duration.get()).toEpochSecond()) {
            return true;
        }

        return count.get() == 0;
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

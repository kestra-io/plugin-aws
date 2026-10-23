package io.kestra.plugin.aws.sqs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.aws.sqs.model.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from SQS",
    description = "Polls a queue until maxRecords or maxDuration is reached, stores messages to internal storage, then optionally deletes them from the queue."
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
                    maxRecords: 10
                """
        )
    },
    metrics = {
        @Metric(
            name = "sqs.consume.messages",
            type = Counter.TYPE,
            unit = "messages",
            description = "Number of messages consumed from the SQS queue."
        )
    }
)
public class Consume extends AbstractSqs implements RunnableTask<Consume.Output> {
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isStopped = new AtomicBoolean(false);

    @Schema(
        title = "Max records",
        description = "Stop after consuming this many messages."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> maxRecords;

    @Schema(
        title = "Max duration",
        description = "Stop after this duration elapses."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration;

    @Builder.Default
    @NotNull
    @Schema(title = "The serializer/deserializer to use")
    @PluginProperty(group = "advanced")
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Schema(
        title = "Auto-delete",
        description = "If true (default), delete the messages from the queue once they are stored in internal storage. A run that fails or is killed deletes nothing, so its messages are delivered again after the visibility timeout."
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<Boolean> autoDelete = Property.ofValue(true);

    @Schema(
        title = "Visibility timeout",
        description = "Seconds a received message stays hidden; default 30s. With `autoDelete`, a run longer than this extends the timeout of the messages it holds until they are deleted (needs `sqs:ChangeMessageVisibility`); if that fails, the run stops receiving early."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> visibilityTimeout = Property.ofValue(30);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (var sqsClient = this.client(runContext)) {
            return run(runContext, sqsClient);
        }
    }

    @SuppressWarnings("BusyWait")
    Output run(RunContext runContext, SqsClient sqsClient) throws Exception {
        var queueUrl = runContext.render(getQueueUrl()).as(String.class).orElseThrow();
        if (this.maxDuration == null && this.maxRecords == null) {
            throw new IllegalArgumentException("'maxDuration' or 'maxRecords' must be set to avoid an infinite loop");
        }

        var rSerdeType = runContext.render(serdeType).as(SerdeType.class).orElseThrow();
        var rAutoDelete = runContext.render(autoDelete).as(Boolean.class).orElse(true);
        var rVisibilityTimeout = runContext.render(visibilityTimeout).as(Integer.class).orElse(30);

        var total = new AtomicInteger();
        var started = ZonedDateTime.now();
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        // Message id -> latest receipt handle of the messages held by this run until they are stored and deleted.
        var receiptHandles = new LinkedHashMap<String, String>();
        var halfVisibility = Duration.ofSeconds(rVisibilityTimeout).dividedBy(2);
        var visibilityExtendedAt = Instant.now();
        var stopReceiving = false;

        try (var outputFile = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            do {
                var receiveRequest = ReceiveMessageRequest.builder()
                    .waitTimeSeconds(1) // this would avoid generating too many calls if there are no messages
                    .queueUrl(queueUrl)
                    .visibilityTimeout(rVisibilityTimeout)
                    .build();
                var msg = sqsClient.receiveMessage(receiveRequest);
                for (var m : msg.messages()) {
                    if (rAutoDelete && receiptHandles.put(m.messageId(), m.receiptHandle()) != null) {
                        // Already held by this run, so its visibility timeout elapsed: keep the newest receipt handle
                        // (the only one SQS deletes with) and stop receiving before more messages come back.
                        runContext.logger().warn("Message {} was received again before it could be deleted, stopping receiving", m.messageId());
                        stopReceiving = true;
                        continue;
                    }
                    FileSerde.write(outputFile, rSerdeType.deserialize(m.body()));
                    total.getAndIncrement();
                }

                // Keep the held messages hidden until they are deleted, so a long run does not receive them again.
                if (
                    rAutoDelete && !stopReceiving && !receiptHandles.isEmpty()
                        && Duration.between(visibilityExtendedAt, Instant.now()).compareTo(halfVisibility) >= 0
                ) {
                    stopReceiving = !extendVisibility(sqsClient, queueUrl, new ArrayList<>(receiptHandles.values()), rVisibilityTimeout, runContext);
                    visibilityExtendedAt = Instant.now();
                }

                Thread.sleep(100);
            } while (!stopReceiving && !this.ended(total, started, runContext));

            runContext.metric(Counter.of("sqs.consume.messages", total.get(), "queue", queueUrl));
            outputFile.flush();
        }

        // Messages are only deleted once they are stored: any failure up to here leaves them all in the queue.
        var uri = runContext.storage().putFile(tempFile);

        // A killed run may not publish its output, so its messages are left in the queue to be delivered again.
        var failedDeletes = rAutoDelete && !isStopped.get()
            ? deleteMessages(sqsClient, queueUrl, new ArrayList<>(receiptHandles.entrySet()), runContext)
            : 0;

        return Output.builder()
            .uri(uri)
            .count(total.get())
            .failedDeletesCount(failedDeletes)
            .build();
    }

    /**
     * Pushes back the visibility timeout of the held messages, in batches of 10 (the SQS limit). Returns false when
     * that fails, so the caller stops receiving and deletes the stored messages before they become visible again.
     */
    private boolean extendVisibility(SqsClient sqsClient, String queueUrl, List<String> receiptHandles, int visibilityTimeout, RunContext runContext) {
        for (int from = 0; from < receiptHandles.size(); from += 10) {
            var batch = receiptHandles.subList(from, Math.min(from + 10, receiptHandles.size()));
            var entries = new ArrayList<ChangeMessageVisibilityBatchRequestEntry>(batch.size());
            for (int i = 0; i < batch.size(); i++) {
                entries.add(
                    ChangeMessageVisibilityBatchRequestEntry.builder()
                        .id(String.valueOf(i))
                        .receiptHandle(batch.get(i))
                        .visibilityTimeout(visibilityTimeout)
                        .build()
                );
            }

            try {
                var response = sqsClient.changeMessageVisibilityBatch(
                    ChangeMessageVisibilityBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(entries)
                        .build()
                );
                if (!response.failed().isEmpty()) {
                    runContext.logger().warn(
                        "Could not extend the visibility timeout of {} message(s), stopping receiving: {}",
                        response.failed().size(), response.failed().stream().map(f -> f.code() + " " + f.message()).toList()
                    );
                    return false;
                }
            } catch (SdkException e) {
                runContext.logger().warn("Could not extend the visibility timeout of the received messages, stopping receiving", e);
                return false;
            }
        }
        return true;
    }

    /**
     * Deletes the stored messages in batches of 10 (the SQS limit) and returns how many could not be deleted.
     * Failures do not fail the task: the messages are already stored, and the ones not deleted stay in the queue.
     */
    private int deleteMessages(SqsClient sqsClient, String queueUrl, List<Map.Entry<String, String>> messages, RunContext runContext) {
        var failed = 0;
        for (int from = 0; from < messages.size(); from += 10) {
            var batch = messages.subList(from, Math.min(from + 10, messages.size()));
            var entries = new ArrayList<DeleteMessageBatchRequestEntry>(batch.size());
            for (int i = 0; i < batch.size(); i++) {
                entries.add(
                    DeleteMessageBatchRequestEntry.builder()
                        .id(String.valueOf(i))
                        .receiptHandle(batch.get(i).getValue())
                        .build()
                );
            }

            DeleteMessageBatchResponse response;
            try {
                response = sqsClient.deleteMessageBatch(
                    DeleteMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(entries)
                        .build()
                );
            } catch (SdkException e) {
                runContext.logger().warn(
                    "SQS batch delete failed for {} message(s), they stay in the queue and will be delivered again: {}",
                    batch.size(), batch.stream().map(Map.Entry::getKey).toList(), e
                );
                failed += batch.size();
                continue;
            }

            if (!response.failed().isEmpty()) {
                var failedDetails = response.failed().stream()
                    .map(f -> "messageId=" + batch.get(Integer.parseInt(f.id())).getKey() + " code=" + f.code() + " message=" + f.message())
                    .toList();
                runContext.logger().warn(
                    "SQS batch delete had {} failure(s), these messages stay in the queue and will be delivered again: {}",
                    response.failed().size(), failedDetails
                );
                failed += response.failed().size();
            }
        }
        return failed;
    }

    @Override
    public void kill() {
        isStopped.set(true);
    }

    private boolean ended(AtomicInteger count, ZonedDateTime start, RunContext runContext) throws IllegalVariableEvaluationException {
        if (isStopped.get()) {
            return true;
        }

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
            title = "Consumed count",
            description = "Messages read within limits."
        )
        private final Integer count;
        @Schema(
            title = "Messages file URI",
            description = "Internal storage URI with serialized messages."
        )
        private final URI uri;
        @Schema(
            title = "Messages not deleted",
            description = "Stored messages whose delete failed; they stay in the queue and are delivered again. The task ends in WARNING when this is not 0."
        )
        private final Integer failedDeletesCount;

        @Override
        public Optional<State.Type> finalState() {
            return failedDeletesCount != null && failedDeletesCount > 0 ? Optional.of(State.Type.WARNING) : io.kestra.core.models.tasks.Output.super.finalState();
        }
    }
}

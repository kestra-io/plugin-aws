package io.kestra.plugin.aws.sqs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

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
import io.kestra.core.utils.ListUtils;
import io.kestra.plugin.aws.sqs.model.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from SQS",
    description = """
        Polls a queue until `maxRecords` or `maxDuration` is reached, stores messages to internal storage, then optionally deletes them from the queue."""
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
    static final int MAX_VISIBILITY_TIMEOUT = 43200;

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
        description = "Keep polling until this duration elapses, even if the queue is empty, or until maxRecords is reached."
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
        description = """
            If true (default), delete the messages from the queue once they are stored in internal storage.
            Messages are held until the run ends, so a run that fails or is killed deletes nothing and its messages are delivered again once hidden time runs out.
            Set `maxRecords` to bound how many messages a run holds, since SQS caps in-flight messages per queue.
            The task ends in WARNING when some deletes fail, and fails when none succeed."""
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<Boolean> autoDelete = Property.ofValue(true);

    @Schema(
        title = "Visibility timeout",
        description = """
            Seconds a received message stays hidden, from 0 to 43200. Default 30s.
            With `autoDelete` and `maxDuration`, received messages stay hidden for the rest of the run plus this value, so the run has this many seconds after `maxDuration` to store and delete them. Past the 43200 cap, a message comes back mid-run and the run stops receiving.
            With `autoDelete` and no `maxDuration`, a run stops receiving when one of its messages comes back, so it receives for about this many seconds at most."""
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<@Min(0) @Max(MAX_VISIBILITY_TIMEOUT) Integer> visibilityTimeout = Property.ofValue(30);

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

        var rSerdeType = runContext.render(serdeType).as(SerdeType.class).orElse(SerdeType.STRING);
        var rAutoDelete = runContext.render(autoDelete).as(Boolean.class).orElse(true);
        var rVisibilityTimeout = runContext.render(visibilityTimeout).as(Integer.class).orElse(30);
        var rMaxDuration = runContext.render(maxDuration).as(Duration.class).orElse(null);
        var rMaxRecords = runContext.render(maxRecords).as(Integer.class).orElse(null);

        var total = new AtomicInteger();
        var started = ZonedDateTime.now();
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        var receiptHandleByMessageId = new LinkedHashMap<String, String>();
        var stopReceiving = false;

        try (var outputFile = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            do {
                var receiveRequest = ReceiveMessageRequest.builder()
                    .waitTimeSeconds(1) // this would avoid generating too many calls if there are no messages
                    .queueUrl(queueUrl)
                    .visibilityTimeout(receiveVisibility(rAutoDelete, rVisibilityTimeout, rMaxDuration, started))
                    .build();
                var msg = sqsClient.receiveMessage(receiveRequest);
                for (var m : msg.messages()) {
                    var previousHandle = rAutoDelete ? receiptHandleByMessageId.put(m.messageId(), m.receiptHandle()) : null;
                    // Hidden time ran out: already stored, keep the newest receipt handle for the delete
                    if (previousHandle != null) {
                        stopReceiving = true;
                        continue;
                    }
                    FileSerde.write(outputFile, rSerdeType.deserialize(m.body()));
                    total.getAndIncrement();
                }

                Thread.sleep(100);
            } while (!stopReceiving && !this.ended(total, started, rMaxRecords, rMaxDuration));

            runContext.metric(Counter.of("sqs.consume.messages", total.get(), "queue", queueUrl));
            outputFile.flush();
        }

        // Messages are only deleted once they are stored: any failure up to here leaves them all in the queue.
        var uri = runContext.storage().putFile(tempFile);

        // A killed run may not publish its output, so its messages are left in the queue to be delivered again.
        var failedDeletes = !isStopped.get()
            ? deleteMessages(sqsClient, queueUrl, List.copyOf(receiptHandleByMessageId.entrySet()), runContext)
            : 0;
        if (failedDeletes > 0 && failedDeletes == receiptHandleByMessageId.size()) {
            throw new IllegalStateException(
                "None of the %d stored messages could be deleted from %s, so they stay in the queue. Check that the role has `sqs:DeleteMessage`.".formatted(failedDeletes, queueUrl)
            );
        }

        return Output.builder()
            .uri(uri)
            .count(total.get())
            .failedDeletesCount(failedDeletes)
            .build();
    }

    // Returns how many deletes failed: the messages are stored, the ones not deleted stay in the queue
    private int deleteMessages(SqsClient sqsClient, String queueUrl, List<Map.Entry<String, String>> messages, RunContext runContext) {
        var failed = 0;
        // SQS batch calls accept at most 10 entries
        for (var batch : ListUtils.partition(messages, 10)) {
            var entries = IntStream.range(0, batch.size())
                .mapToObj(
                    i -> DeleteMessageBatchRequestEntry.builder()
                        .id(String.valueOf(i))
                        .receiptHandle(batch.get(i).getValue())
                        .build()
                )
                .toList();

            try {
                var response = sqsClient.deleteMessageBatch(
                    DeleteMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(entries)
                        .build()
                );
                if (!response.failed().isEmpty()) {
                    var failedDetails = response.failed().stream()
                        .map(f -> "messageId=%s code=%s message=%s".formatted(batch.get(Integer.parseInt(f.id())).getKey(), f.code(), f.message()))
                        .toList();
                    runContext.logger().warn(
                        "SQS batch delete had {} failure(s), these messages stay in the queue and will be delivered again: {}",
                        response.failed().size(), failedDetails
                    );
                    failed += response.failed().size();
                }
            } catch (SdkException e) {
                runContext.logger().warn(
                    "SQS batch delete failed for {} message(s), they stay in the queue and will be delivered again: {}",
                    batch.size(), batch.stream().map(Map.Entry::getKey).toList(), e
                );
                failed += batch.size();
            }
        }
        return failed;
    }

    // Hides held messages until the run ends plus visibilityTimeout, so no one else receives them before they are stored and deleted
    private static int receiveVisibility(boolean rAutoDelete, int rVisibilityTimeout, Duration rMaxDuration, ZonedDateTime started) {
        if (!rAutoDelete || rMaxDuration == null) {
            return rVisibilityTimeout;
        }
        var remaining = Math.max(0, Duration.between(ZonedDateTime.now(), started.plus(rMaxDuration)).toSeconds());
        return (int) Math.min(remaining + rVisibilityTimeout, MAX_VISIBILITY_TIMEOUT);
    }

    @Override
    public void kill() {
        isStopped.set(true);
    }

    private boolean ended(AtomicInteger count, ZonedDateTime start, Integer rMaxRecords, Duration rMaxDuration) {
        if (isStopped.get()) {
            return true;
        }

        if (rMaxRecords != null && count.get() >= rMaxRecords) {
            return true;
        }

        // With maxDuration set, keep polling until it elapses even on an empty queue, instead of stopping on the
        // first empty poll (#860, fixes #852); without it, stop once a poll returns nothing.
        if (rMaxDuration != null) {
            return ZonedDateTime.now().toEpochSecond() > start.plus(rMaxDuration).toEpochSecond();
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
            description = """
                Stored messages whose delete failed. They stay in the queue and are delivered again.
                The task ends in WARNING when some deletes fail, and fails when none succeed."""
        )
        private final int failedDeletesCount;

        @Override
        public Optional<State.Type> finalState() {
            return failedDeletesCount > 0 ? Optional.of(State.Type.WARNING) : Optional.empty();
        }
    }
}

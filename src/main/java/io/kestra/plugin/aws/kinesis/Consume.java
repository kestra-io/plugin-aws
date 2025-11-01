package io.kestra.plugin.aws.kinesis;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Consume messages from a Kinesis stream",
            code = """
                id: kinesis_consume
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.aws.kinesis.Consume
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    streamName: "mystream"
                    startingPosition: "TRIM_HORIZON"
                    pollDuration: PT5S
                    maxRecords: 1000
            """
        ),
        @Example(
            full = true,
            title = "Consume messages from a specific timestamp",
            code = """
                id: kinesis_consume_timestamp
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.aws.kinesis.Consume
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    streamName: "mystream"
                    startingPosition: "AT_TIMESTAMP"
                    startingTimestamp: "2024-01-01T00:00:00Z"
                    maxDuration: PT10M
            """
        )
    },
    metrics = {
        @Metric(
            name = "records.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Total number of records consumed from Kinesis."
        ),
        @Metric(
            name = "duration",
            type = Timer.TYPE,
            unit = "nanoseconds",
            description = "Execution time for the Consume task."
        )
    }
)
@Schema(title = "Consume messages from an AWS Kinesis stream.")
public class Consume extends AbstractConnection implements RunnableTask<Consume.Output> {
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.ALWAYS);

    @PluginProperty
    @Schema(title = "The name of the stream to consume records from.")
    private Property<String> streamName;

    @PluginProperty
    @Schema(title = "The ARN of the stream to consume records from.")
    private Property<String> streamArn;

    @Builder.Default
    @PluginProperty
    @Schema(
        title = "Maximum number of records to retrieve per shard per poll.",
        description = "The maximum number of records that will be retrieved from each shard in a single GetRecords call."
    )
    private Property<Integer> maxRecords = Property.ofValue(1000);

    @Builder.Default
    @PluginProperty
    @Schema(
        title = "How long to poll for records.",
        description = "The duration to wait between polling each shard for new records."
    )
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(5));

    @PluginProperty
    @Schema(
        title = "Maximum total duration for consuming records.",
        description = "If set, the task will stop consuming after this duration, regardless of whether records are still available."
    )
    private Property<Duration> maxDuration;

    @PluginProperty
    @Schema(
        title = "Maximum total number of records to consume.",
        description = "If set, the task will stop consuming after reaching this number of records."
    )
    private Property<Integer> maxRecordsTotal;

    @Builder.Default
    @NotNull
    @PluginProperty
    @Schema(title = "Where to start consuming from. Possible values: TRIM_HORIZON, LATEST, AT_TIMESTAMP.")
    private Property<String> startingPosition = Property.ofValue("TRIM_HORIZON");

    @PluginProperty
    @Schema(title = "Timestamp for starting position, only used if startingPosition is AT_TIMESTAMP.")
    private Property<String> startingTimestamp;

    @Override
    public Output run(RunContext runContext) throws Exception {
        final long start = System.nanoTime();

        String stream = runContext.render(streamName).as(String.class).orElse(null);
        String streamArnValue = runContext.render(streamArn).as(String.class).orElse(null);

        if (stream == null && streamArnValue == null) {
            throw new IllegalArgumentException("Either streamName or streamArn must be provided.");
        }

        KinesisClient client = client(runContext);

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        AtomicInteger total = new AtomicInteger();
        Map<String, Integer> countByShard = new HashMap<>();
        ZonedDateTime started = ZonedDateTime.now();

        try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            ListShardsRequest.Builder listShardsBuilder = ListShardsRequest.builder();
            if (stream != null) {
                listShardsBuilder.streamName(stream);
            } else {
                listShardsBuilder.streamARN(streamArnValue);
            }

            List<Shard> shards = client.listShards(listShardsBuilder.build()).shards();

            if (shards.isEmpty()) {
                runContext.logger().warn("No shards found for stream: {}", stream != null ? stream : streamArnValue);
            }

            outerLoop:
            for (Shard shard : shards) {
                if (ended(runContext, total, started)) {
                    break;
                }

                String shardIterator = getShardIterator(runContext, client, stream, streamArnValue, shard);

                while (shardIterator != null && !ended(runContext, total, started)) {
                    GetRecordsResponse response = client.getRecords(
                        GetRecordsRequest.builder()
                            .shardIterator(shardIterator)
                            .limit(runContext.render(maxRecords).as(Integer.class).orElse(1000))
                            .build()
                    );

                    List<software.amazon.awssdk.services.kinesis.model.Record> records = response.records();

                    if (!records.isEmpty()) {
                        // Process records one by one and check limit after each
                        for (software.amazon.awssdk.services.kinesis.model.Record record : records) {
                            // Check if we've reached the limit BEFORE processing this record
                            if (ended(runContext, total, started)) {
                                break outerLoop;
                            }

                            FileSerde.write(output, new Message(
                                record.partitionKey(),
                                new String(record.data().asByteArray(), StandardCharsets.UTF_8),
                                shard.shardId(),
                                Instant.ofEpochMilli(record.approximateArrivalTimestamp().toEpochMilli()),
                                record.sequenceNumber()
                            ));
                            total.incrementAndGet();
                            countByShard.merge(shard.shardId(), 1, Integer::sum);
                        }
                    }

                    shardIterator = response.nextShardIterator();

                    // If no more records or shard is closed, break
                    if (records.isEmpty() || shardIterator == null) {
                        break;
                    }

                    // Sleep between polls to avoid throttling
                    Thread.sleep(runContext.render(pollDuration).as(Duration.class).orElse(Duration.ofSeconds(5)).toMillis());
                }
            }

            output.flush();
        }

        URI uri = runContext.storage().putFile(tempFile);

        runContext.metric(Counter.of("records.count", total.get()));
        countByShard.forEach((shardId, count) ->
            runContext.metric(Counter.of("records.count", count, "shard", shardId))
        );
        runContext.metric(Timer.of("duration", Duration.ofNanos(System.nanoTime() - start)));

        return Output.builder()
            .uri(uri)
            .messagesCount(total.get())
            .build();
    }

    private boolean ended(RunContext runContext, AtomicInteger count, ZonedDateTime start) throws IllegalVariableEvaluationException {
        // Check max records
        final Optional<Integer> renderedMaxRecords = runContext.render(this.maxRecordsTotal).as(Integer.class);
        if (renderedMaxRecords.isPresent() && count.get() >= renderedMaxRecords.get()) {
            return true;
        }

        // Check max duration
        final Optional<Duration> renderedMaxDuration = runContext.render(this.maxDuration).as(Duration.class);
        return renderedMaxDuration.isPresent() &&
            ZonedDateTime.now().toEpochSecond() >= start.plus(renderedMaxDuration.get()).toEpochSecond();
    }

    private String getShardIterator(RunContext runContext, KinesisClient client, String stream, String streamArnValue, Shard shard) throws IllegalVariableEvaluationException {
        String position = runContext.render(startingPosition).as(String.class).orElse("TRIM_HORIZON");

        GetShardIteratorRequest.Builder builder = GetShardIteratorRequest.builder()
            .shardId(shard.shardId());

        if (stream != null) {
            builder.streamName(stream);
        } else {
            builder.streamARN(streamArnValue);
        }

        switch (position.toUpperCase()) {
            case "LATEST" -> builder.shardIteratorType(ShardIteratorType.LATEST);
            case "AT_TIMESTAMP" -> {
                String ts = runContext.render(startingTimestamp).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException(
                        "startingTimestamp must be provided when startingPosition is AT_TIMESTAMP"
                    ));
                builder.shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                    .timestamp(Instant.parse(ts));
            }
            default -> builder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        }

        return client.getShardIterator(builder.build()).shardIterator();
    }

    protected KinesisClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        var clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, KinesisClient.builder()).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of a file in Kestra's internal storage containing the consumed messages."
        )
        private URI uri;

        @Schema(title = "Number of messages consumed from Kinesis.")
        private int messagesCount;
    }

    @Getter
    @AllArgsConstructor
    static class Message {
        @Schema(title = "The partition key of the record.")
        private String key;

        @Schema(title = "The data of the record.")
        private String value;

        @Schema(title = "The shard ID from which the record was consumed.")
        private String shardId;

        @Schema(title = "The approximate arrival timestamp of the record.")
        private Instant timestamp;

        @Schema(title = "The sequence number of the record.")
        private String sequenceNumber;
    }
}
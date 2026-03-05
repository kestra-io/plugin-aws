package io.kestra.plugin.aws.kinesis;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume records from Kinesis",
    description = "Reads records from a stream starting at the chosen iterator type. Stops when maxRecords or maxDuration is reached. Writes results to internal storage and records last sequence per shard."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume records from a Kinesis stream using TRIM_HORIZON",
            full = true,
            code = """
                id: kinesis_consume
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.aws.kinesis.Consume
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    streamName: "stream"
                    iteratorType: TRIM_HORIZON
                    pollDuration: PT5S
                    maxRecords: 100
                """
        )
    }
)
public class Consume extends AbstractKinesis implements RunnableTask<Consume.Output> {
    @NotNull
    @Schema(
        title = "Stream name",
        description = "Name of the Kinesis stream to read from."
    )
    private Property<String> streamName;

    @Builder.Default
    @Schema(
        title = "Iterator type",
        description = "Start position: LATEST, TRIM_HORIZON, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, AT_TIMESTAMP."
    )
    private Property<IteratorType> iteratorType = Property.ofValue(IteratorType.LATEST);

    @Schema(
        title = "Starting sequence number",
        description = "Required when iteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER."
    )
    private Property<String> startingSequenceNumber;

    @Builder.Default
    @Schema(
        title = "Max records",
        description = "Stop after consuming this many records; default 1000."
    )
    private Property<Integer> maxRecords = Property.ofValue(1000);

    @Builder.Default
    @Schema(
        title = "Max duration",
        description = "Stop after this duration elapses; default 30s."
    )
    private Property<Duration> maxDuration = Property.ofValue(Duration.ofSeconds(30));

    @Builder.Default
    @Schema(
        title = "Poll interval",
        description = "Sleep between GetRecords calls; default 1s."
    )
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(1));

    @Override
    public Output run(RunContext runContext) throws Exception {
        long startedAt = System.nanoTime();
        var rStream = runContext.render(this.streamName).as(String.class).orElseThrow();

        KinesisClient client = client(runContext);

        List<Shard> shards = client.listShards(ListShardsRequest.builder().streamName(rStream).build()).shards();

        File outFile = runContext.workingDir().createTempFile(".ion").toFile();
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(outFile));

        int consumed = 0;
        Map<String, String> lastSequence = new HashMap<>();
        var rMaxDuration = Instant.now().plus(runContext.render(maxDuration).as(Duration.class).orElse(Duration.ofSeconds(30)));

        for (Shard shard : shards) {
            String iterator = buildShardIterator(runContext, client, rStream, shard);

            while (iterator != null) {
                GetRecordsResponse response = client.getRecords(
                    GetRecordsRequest.builder()
                        .shardIterator(iterator)
                        .limit(1000)
                        .build()
                );

                for (Record record : response.records()) {
                    ConsumedRecord consumedRecord = ConsumedRecord.builder()
                        .data(new String(record.data().asByteArray()))
                        .partitionKey(record.partitionKey())
                        .sequenceNumber(record.sequenceNumber())
                        .approximateArrivalTimestamp(record.approximateArrivalTimestamp())
                        .shardId(shard.shardId())
                        .build();

                    FileSerde.write(output, consumedRecord);

                    consumed++;
                    lastSequence.put(shard.shardId(), record.sequenceNumber());

                    if (consumed >= runContext.render(maxRecords).as(Integer.class).orElse(Integer.MAX_VALUE)) {
                        break;
                    }
                }

                iterator = response.nextShardIterator();

                if (Instant.now().isAfter(rMaxDuration) ||
                    consumed >= runContext.render(maxRecords).as(Integer.class).orElse(Integer.MAX_VALUE)) {
                    break;
                }

                Thread.sleep(runContext.render(pollDuration).as(Duration.class).orElse(Duration.ofSeconds(1)).toMillis());
            }
        }

        output.flush();
        URI uri = runContext.storage().putFile(outFile);

        // metrics
        runContext.metric(Timer.of("duration", Duration.ofNanos(System.nanoTime() - startedAt)));
        runContext.metric(Counter.of("records", consumed));

        return Output.builder()
            .uri(uri)
            .count(consumed)
            .lastSequencePerShard(lastSequence)
            .build();
    }

    private String buildShardIterator(RunContext runContext, KinesisClient client, String stream, Shard shard) throws IllegalVariableEvaluationException {
        var builder = GetShardIteratorRequest.builder()
            .streamName(stream)
            .shardId(shard.shardId())
            .shardIteratorType(ShardIteratorType.fromValue(
                runContext.render(iteratorType).as(IteratorType.class).orElse(IteratorType.LATEST).name()
            ));

        if (startingSequenceNumber != null) {
            runContext.render(startingSequenceNumber)
                .as(String.class)
                .ifPresent(builder::startingSequenceNumber);
        }

        return client.getShardIterator(builder.build()).shardIterator();
    }

    @Getter
    @Builder
    public static class ConsumedRecord implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The data payload returned by Kinesis.")
        private final String data;

        @Schema(
            title = "Partition key"
        )
        private final String partitionKey;

        @Schema(
            title = "Sequence number"
        )
        private final String sequenceNumber;

        @Schema(
            title = "Shard ID"
        )
        private final String shardId;

        @Schema(
            title = "Approximate arrival timestamp"
        )
        private final Instant approximateArrivalTimestamp;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Records file URI",
            description = "Internal storage URI containing the consumed records (ION)."
        )
        private final URI uri;

        @Schema(
            title = "Record count",
            description = "Total records consumed."
        )
        private final int count;

        @Schema(
            title = "Last sequence per shard",
            description = "Map of shardId to last consumed sequence number."
        )
        private final Map<String, String> lastSequencePerShard;
    }
}

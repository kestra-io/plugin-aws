package io.kestra.plugin.aws.kinesis;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;

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

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.kinesis.model.Record;

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
    },
    metrics = {
        @Metric(name = "records", type = Counter.TYPE, description = "The number of records consumed from the Kinesis stream."),
        @Metric(name = "duration", type = Timer.TYPE, description = "The time spent consuming records from the Kinesis stream.")
    }
)
public class Consume extends AbstractKinesis implements RunnableTask<Consume.Output> {
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isStopped = new AtomicBoolean(false);

    @NotNull
    @Schema(
        title = "Stream name",
        description = "Name of the Kinesis stream to read from."
    )
    @PluginProperty(group = "main")
    private Property<String> streamName;

    @Builder.Default
    @Schema(
        title = "Iterator type",
        description = "Start position: LATEST, TRIM_HORIZON, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, AT_TIMESTAMP."
    )
    @PluginProperty(group = "destination")
    private Property<IteratorType> iteratorType = Property.ofValue(IteratorType.LATEST);

    @Schema(
        title = "Starting sequence number",
        description = "Required when iteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER."
    )
    @PluginProperty(group = "advanced")
    private Property<String> startingSequenceNumber;

    @Builder.Default
    @Schema(
        title = "Max records",
        description = "Stop after consuming this many records; default 1000."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> maxRecords = Property.ofValue(1000);

    @Builder.Default
    @Schema(
        title = "Max duration",
        description = "Stop after this duration elapses; default 30s."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration = Property.ofValue(Duration.ofSeconds(30));

    @Builder.Default
    @Schema(
        title = "Poll interval",
        description = "Sleep between polling rounds; default 1s."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(1));

    @Override
    public void kill() {
        isStopped.set(true);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return run(runContext, Map.of());
    }

    Output run(RunContext runContext, Map<String, String> startingPositionPerShard) throws Exception {
        return run(runContext, startingPositionPerShard, List.of());
    }

    Output run(RunContext runContext, Map<String, String> startingPositionPerShard, List<String> preferredShardOrder) throws Exception {
        long startedAt = System.nanoTime();
        var rStream = runContext.render(this.streamName).as(String.class).orElseThrow();

        KinesisClient client = client(runContext);

        List<Shard> shards = client.listShards(ListShardsRequest.builder().streamName(rStream).build()).shards();
        var startingPositions = startingPositionPerShard == null ? Map.<String, String> of() : startingPositionPerShard;
        var rIteratorType = runContext.render(iteratorType).as(IteratorType.class).orElse(IteratorType.LATEST);
        var pollStartedAt = Instant.now().minus(Duration.ofSeconds(2));

        File outFile = runContext.workingDir().createTempFile(".ion").toFile();
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(outFile));

        int consumed = 0;
        var lastSequence = new HashMap<String, String>();
        var resumePositionPerShard = new HashMap<String, String>();
        var rMaxDuration = Instant.now().plus(runContext.render(maxDuration).as(Duration.class).orElse(Duration.ofSeconds(30)));
        var rMaxRecords = runContext.render(maxRecords).as(Integer.class).orElse(Integer.MAX_VALUE);
        var rPollDuration = runContext.render(pollDuration).as(Duration.class).orElse(Duration.ofSeconds(1));

        var preferredIndex = new HashMap<String, Integer>();
        for (int i = 0; i < preferredShardOrder.size(); i++) {
            preferredIndex.put(preferredShardOrder.get(i), i);
        }

        List<ShardCursor> cursors = new ArrayList<>(shards.size());
        for (Shard shard : shards) {
            var shardStart = buildShardIterator(runContext, client, rStream, shard, startingPositions.get(shard.shardId()));
            cursors.add(new ShardCursor(shard, shardStart.iterator(), shardStart.startingPosition()));
        }
        cursors.sort(Comparator.comparingInt(cursor -> preferredIndex.getOrDefault(cursor.shard.shardId(), -1)));

        var cursorByShardId = new HashMap<String, ShardCursor>();
        for (ShardCursor cursor : cursors) {
            cursorByShardId.put(cursor.shard.shardId(), cursor);
        }

        while (true) {
            int readableShards = 0;
            for (ShardCursor cursor : cursors) {
                if (isReadable(cursor, cursorByShardId)) {
                    readableShards++;
                }
            }

            if (readableShards == 0 || isStopped.get() || consumed >= rMaxRecords) {
                break;
            }

            long remaining = (long) rMaxRecords - consumed;
            int quota = (int) Math.min(Integer.MAX_VALUE, (remaining + readableShards - 1) / readableShards);
            boolean fetched = false;

            for (ShardCursor cursor : cursors) {
                if (isStopped.get() || consumed >= rMaxRecords) {
                    break;
                }
                if (!isReadable(cursor, cursorByShardId)) {
                    continue;
                }

                fetched = true;
                cursor.shardRead = true;

                GetRecordsResponse response = client.getRecords(
                    GetRecordsRequest.builder()
                        .shardIterator(cursor.iterator)
                        .limit((int) Math.min(1000, Math.min(quota, (long) rMaxRecords - consumed)))
                        .build()
                );
                cursor.millisBehindLatest = response.millisBehindLatest();

                int taken = 0;
                for (Record record : response.records()) {
                    if (consumed >= rMaxRecords || taken >= quota) {
                        break;
                    }

                    ConsumedRecord consumedRecord = ConsumedRecord.builder()
                        .data(new String(record.data().asByteArray()))
                        .partitionKey(record.partitionKey())
                        .sequenceNumber(record.sequenceNumber())
                        .approximateArrivalTimestamp(record.approximateArrivalTimestamp())
                        .shardId(cursor.shard.shardId())
                        .build();

                    FileSerde.write(output, consumedRecord);

                    consumed++;
                    taken++;
                    cursor.lastPosition = record.sequenceNumber();
                }

                if (taken < response.records().size()) {
                    cursor.iterator = shardIteratorAfter(client, rStream, cursor.shard.shardId(), cursor.lastPosition);
                } else {
                    cursor.iterator = response.nextShardIterator();
                }

                if (isStopped.get() || consumed >= rMaxRecords || Instant.now().isAfter(rMaxDuration)) {
                    break;
                }
            }

            if (!fetched || isStopped.get() || consumed >= rMaxRecords || Instant.now().isAfter(rMaxDuration)) {
                break;
            }

            Thread.sleep(rPollDuration.toMillis());
        }

        for (ShardCursor cursor : cursors) {
            if (!cursor.shardRead) {
                continue;
            }
            if (cursor.lastPosition != null) {
                lastSequence.put(cursor.shard.shardId(), cursor.lastPosition);
                resumePositionPerShard.put(cursor.shard.shardId(), cursor.lastPosition);
            } else if (cursor.startingPosition != null) {
                resumePositionPerShard.put(cursor.shard.shardId(), cursor.startingPosition);
            } else if (rIteratorType == IteratorType.LATEST || (cursor.millisBehindLatest != null && cursor.millisBehindLatest == 0L)) {
                resumePositionPerShard.put(cursor.shard.shardId(), timestampPosition(pollStartedAt));
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
            .resumePositionPerShard(resumePositionPerShard)
            .build();
    }

    private ShardStart buildShardIterator(RunContext runContext, KinesisClient client, String stream, Shard shard, String startingPosition) throws IllegalVariableEvaluationException {
        if (startingPosition != null) {
            try {
                return new ShardStart(getShardIterator(runContext, client, stream, shard, startingPosition), startingPosition);
            } catch (InvalidArgumentException e) {
                runContext.logger().warn(
                    "Saved position '{}' rejected for shard '{}' in stream '{}', falling back to configured iterator type: {}",
                    startingPosition, shard.shardId(), stream, e.getMessage()
                );
                return new ShardStart(getShardIterator(runContext, client, stream, shard, null), null);
            }
        }

        return new ShardStart(getShardIterator(runContext, client, stream, shard, null), null);
    }

    private record ShardStart(String iterator, String startingPosition) {
    }

    private String shardIteratorAfter(KinesisClient client, String stream, String shardId, String sequenceNumber) {
        return client.getShardIterator(
            GetShardIteratorRequest.builder()
                .streamName(stream)
                .shardId(shardId)
                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .startingSequenceNumber(sequenceNumber)
                .build()
        ).shardIterator();
    }

    private static boolean isReadable(ShardCursor cursor, Map<String, ShardCursor> cursorByShardId) {
        return cursor.iterator != null
            && !hasActiveParent(cursor.shard.parentShardId(), cursorByShardId)
            && !hasActiveParent(cursor.shard.adjacentParentShardId(), cursorByShardId);
    }

    private static boolean hasActiveParent(String parentShardId, Map<String, ShardCursor> cursorByShardId) {
        if (parentShardId == null) {
            return false;
        }

        ShardCursor parent = cursorByShardId.get(parentShardId);
        return parent != null && parent.iterator != null;
    }

    private static final class ShardCursor {
        private final Shard shard;
        private final String startingPosition;
        private String iterator;
        private String lastPosition;
        private Long millisBehindLatest;
        private boolean shardRead;

        private ShardCursor(Shard shard, String iterator, String startingPosition) {
            this.shard = shard;
            this.iterator = iterator;
            this.startingPosition = startingPosition;
        }
    }

    private String getShardIterator(RunContext runContext, KinesisClient client, String stream, Shard shard, String startingPosition) throws IllegalVariableEvaluationException {
        var savedSequence = startingPosition != null && !startingPosition.startsWith("timestamp:") ? startingPosition : null;
        var savedTimestamp = startingPosition != null && startingPosition.startsWith("timestamp:")
            ? Instant.parse(startingPosition.substring("timestamp:".length()))
            : null;
        var builder = GetShardIteratorRequest.builder()
            .streamName(stream)
            .shardId(shard.shardId())
            .shardIteratorType(
                savedSequence != null
                    ? ShardIteratorType.AFTER_SEQUENCE_NUMBER
                    : savedTimestamp != null
                        ? ShardIteratorType.AT_TIMESTAMP
                        : ShardIteratorType.fromValue(runContext.render(iteratorType).as(IteratorType.class).orElse(IteratorType.LATEST).name())
            );

        if (savedSequence != null) {
            builder.startingSequenceNumber(savedSequence);
        } else if (savedTimestamp != null) {
            builder.timestamp(savedTimestamp);
        } else if (startingSequenceNumber != null) {
            runContext.render(startingSequenceNumber)
                .as(String.class)
                .ifPresent(builder::startingSequenceNumber);
        }

        return client.getShardIterator(builder.build()).shardIterator();
    }

    private static String timestampPosition(Instant timestamp) {
        return "timestamp:" + timestamp;
    }

    @Getter
    @Builder
    public static class ConsumedRecord implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The data payload returned by Kinesis")
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

        @JsonIgnore
        private final Map<String, String> resumePositionPerShard;
    }
}

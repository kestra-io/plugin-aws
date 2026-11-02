package io.kestra.plugin.aws.kinesis;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.kinesis.model.Record;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Send multiple records as maps to Amazon Kinesis Data Streams. Check the following AWS API reference for the structure of the [PutRecordsRequestEntry](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecordsRequestEntry.html) request payload",
            full = true,
            code = """
                id: aws_kinesis_put_records
                namespace: company.team

                tasks:
                  - id: put_records
                    type: io.kestra.plugin.aws.kinesis.PutRecords
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    streamName: "mystream"
                    records:
                      - data: "user sign-in event"
                        explicitHashKey: "optional hash value overriding the partition key"
                        partitionKey: "user1"
                      - data: "user sign-out event"
                        partitionKey: "user1"
                """
        ),
        @Example(
            title = "Send multiple records from an internal storage ION file to Amazon Kinesis Data Streams",
            full = true,
            code = """
                id: aws_kinesis_put_records
                namespace: company.team

                tasks:
                  - id: put_records
                    type: io.kestra.plugin.aws.kinesis.PutRecords
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    streamName: "mystream"
                    records: kestra:///myfile.ion
                """
        )
    },
    metrics = {
        @Metric(
            name = "record.count",
            type = Counter.TYPE,
            unit = "records",
            description = "Total number of records attempted to be sent to Kinesis."
        ),
        @Metric(
            name = "record.successful",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records successfully sent to Kinesis."
        ),
        @Metric(
            name = "record.failed",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records that failed to be sent to Kinesis."
        ),
        @Metric(
            name = "duration",
            type = Timer.TYPE,
            unit = "nanoseconds",
            description = "Execution time for the PutRecords task."
        )
    }
)
@Schema(
    title = "Put records to Kinesis Data Streams",
    description = "Sends multiple records to a stream by name or ARN. Records can be inline or loaded from a kestra:// ION file. Records are sent in batches of at most 500 records and 5 MiB, the Kinesis limits for a single PutRecords request, and throttled records are retried up to 3 times with backoff. Records require data and partitionKey; explicitHashKey is optional. Fails the task when any record fails if failOnUnsuccessfulRecords is true (default)."
)
public class PutRecords extends AbstractKinesis implements RunnableTask<PutRecords.Output> {
    private static final ObjectMapper MAPPER = JacksonMapper.ofIon()
        .setSerializationInclusion(JsonInclude.Include.ALWAYS);

    static final int MAX_FAILURES_IN_MESSAGE = 10;

    @NotNull
    @Schema(
        title = "Fail on unsuccessful records",
        description = "If true (default), task fails when at least one record is rejected."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> failOnUnsuccessfulRecords = Property.ofValue(true);

    @Schema(
        title = "Stream name",
        description = "Kinesis stream name; set this or streamArn."
    )
    @PluginProperty(group = "advanced")
    private Property<String> streamName;

    @Schema(
        title = "Stream ARN",
        description = "Kinesis stream ARN; set this or streamName."
    )
    @PluginProperty(group = "advanced")
    private Property<String> streamArn;

    @PluginProperty(dynamic = true, group = "main")
    @Schema(
        title = "Records",
        description = "List of records or kestra:// URI to an ION file of records. Each requires data and partitionKey; optional explicitHashKey.",
        anyOf = { String.class, Record[].class }
    )
    @NotNull
    private Object records;

    @Override
    public Output run(RunContext runContext) throws Exception {
        final var start = System.nanoTime();
        var rFailOnUnsuccessfulRecords = runContext.render(failOnUnsuccessfulRecords).as(Boolean.class).orElse(true);

        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        var summary = new ResultSummary();
        try (
            var client = client(runContext);
            var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var request = requestBuilder(runContext);
            var batcher = new RecordBatcher<Record>(
                entries -> client.putRecords(request.records(entries).build()),
                duration -> Thread.sleep(duration.toMillis()),
                (record, result) -> {
                    summary.add(result);
                    FileSerde.write(output, toOutputEntry(record, result));
                }
            );

            var position = new int[] { 0 };
            forEachRecord(runContext, record -> batcher.add(record, toEntry(runContext, record, position[0]++)));
            batcher.flush();
        }

        if (rFailOnUnsuccessfulRecords && summary.getFailedCount() > 0) {
            var message = summary.failureMessage();
            runContext.logger().error(message);
            throw new RuntimeException(message);
        }

        runContext.metric(Timer.of("duration", Duration.ofNanos(System.nanoTime() - start)));
        runContext.metric(Counter.of("record.failed", summary.getFailedCount()));
        runContext.metric(Counter.of("record.successful", summary.getCount() - summary.getFailedCount()));
        runContext.metric(Counter.of("record.count", summary.getCount()));

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .failedRecordsCount(summary.getFailedCount())
            .recordCount(summary.getCount())
            .build();
    }

    private PutRecordsRequest.Builder requestBuilder(RunContext runContext) throws IllegalVariableEvaluationException {
        var builder = PutRecordsRequest.builder();
        var rStreamArn = runContext.render(streamArn).as(String.class).orElse(null);
        var rStreamName = runContext.render(streamName).as(String.class).orElse(null);
        if (!Strings.isNullOrEmpty(rStreamArn)) {
            return builder.streamARN(rStreamArn);
        }
        if (!Strings.isNullOrEmpty(rStreamName)) {
            return builder.streamName(rStreamName);
        }
        throw new IllegalArgumentException("Either streamName or streamArn has to be set.");
    }

    static PutRecordsRequestEntry toEntry(RunContext runContext, Record record, int position) throws IllegalVariableEvaluationException {
        if (record.getData() == null) {
            throw new IllegalArgumentException("Record at position " + position + " has no data, set it on every record");
        }
        var entry = record.toPutRecordsRequestEntry(runContext);
        if (Strings.isNullOrEmpty(entry.partitionKey())) {
            throw new IllegalArgumentException("Record at position " + position + " has no partitionKey, set it on every record");
        }
        return entry;
    }

    // Reads an ION file lazily so only the current request is held in memory; inline lists are already in the flow.
    private void forEachRecord(RunContext runContext, RecordConsumer consumer) throws Exception {
        if (records instanceof String uri) {
            var from = new URI(runContext.render(uri));
            if (!"kestra".equals(from.getScheme())) {
                throw new IllegalArgumentException("Invalid records parameter, must be a Kestra internal storage URI, or a list of records.");
            }
            try (var inputStream = new BufferedInputStream(runContext.storage().getFile(from), FileSerde.BUFFER_SIZE)) {
                for (var record : FileSerde.readAll(inputStream, Record.class).toIterable()) {
                    consumer.accept(record);
                }
            }
            return;
        }

        if (records instanceof List) {
            List<Record> list = MAPPER.convertValue(records, new TypeReference<>() {
            });
            for (var record : list) {
                consumer.accept(record);
            }
            return;
        }

        throw new IllegalVariableEvaluationException("Invalid records type '" + records.getClass() + "'");
    }

    private static OutputEntry toOutputEntry(Record record, PutRecordsResultEntry result) {
        return OutputEntry.builder()
            .record(record)
            .sequenceNumber(result.sequenceNumber())
            .shardId(result.shardId())
            .errorCode(result.errorCode())
            .errorMessage(result.errorMessage())
            .build();
    }

    @FunctionalInterface
    private interface RecordConsumer {
        void accept(Record record) throws Exception;
    }

    // Counts the results and keeps the first few failures, so the error message stays small for large inputs.
    @Getter
    static class ResultSummary {
        private int count;
        private int failedCount;
        private final List<String> firstFailures = new ArrayList<>();

        void add(PutRecordsResultEntry result) {
            if (result.errorCode() != null) {
                failedCount++;
                if (firstFailures.size() < MAX_FAILURES_IN_MESSAGE) {
                    firstFailures.add("position " + count + ": " + result.errorCode() + " (" + result.errorMessage() + ")");
                }
            }
            count++;
        }

        String failureMessage() {
            return String.format(
                "%d of %d records failed (throttled records were retried up to %d times). First %d failures: %s",
                failedCount, count, RecordBatcher.MAX_RETRIES, firstFailures.size(), String.join("; ", firstFailures)
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The URI of stored data",
            description = "The successfully and unsuccessfully ingested records." +
                "If the ingestion was successful, the output includes the record sequence number." +
                "Otherwise, the output provides the error code and error message for troubleshooting."
        )
        private URI uri;

        @Schema(
            title = "The number of failed records"
        )
        private int failedRecordsCount;

        @Schema(
            title = "The total number of records sent to AWS Kinesis Data Streams"
        )
        private int recordCount;

        @Override
        public Optional<State.Type> finalState() {
            return this.failedRecordsCount > 0 ? Optional.of(State.Type.WARNING) : io.kestra.core.models.tasks.Output.super.finalState();
        }
    }

    @Builder
    @Getter
    public static class OutputEntry {
        @Schema(
            title = "The sequence number for an individual record result"
        )
        private final String sequenceNumber;

        @Schema(
            title = "The shard ID for an individual record result"
        )
        private final String shardId;

        @Schema(
            title = "The error code that indicates the failure"
        )
        private final String errorCode;

        @Schema(
            title = "The error message that explains the failure"
        )
        private final String errorMessage;

        @Schema(
            title = "The original record"
        )
        private final Record record;
    }
}

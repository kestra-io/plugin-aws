package io.kestra.plugin.aws.kinesis;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.core.utils.Rethrow.throwFunction;

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
    description = "Sends multiple records to a stream by name or ARN. Records can be inline or loaded from a kestra:// ION file. Records are sent in batches of at most 500 records and 5 MiB, the Kinesis limits for a single PutRecords request. Records require data and partitionKey; explicitHashKey is optional. Fails the task when any record fails if failOnUnsuccessfulRecords is true (default)."
)
public class PutRecords extends AbstractKinesis implements RunnableTask<PutRecords.Output> {
    private static final ObjectMapper MAPPER = JacksonMapper.ofIon()
        .setSerializationInclusion(JsonInclude.Include.ALWAYS);

    /**
     * Kinesis accepts at most 500 records per PutRecords request.
     */
    static final int MAX_RECORDS_PER_REQUEST = 500;

    /**
     * Kinesis accepts at most 5 MiB per PutRecords request, partition keys included.
     */
    static final int MAX_BYTES_PER_REQUEST = 5 * 1024 * 1024;

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
        final long start = System.nanoTime();

        List<Record> records = getRecordList(this.records, runContext);

        BatchResult result = putRecords(runContext, records);

        // Fail if failOnUnsuccessfulRecords
        if (runContext.render(failOnUnsuccessfulRecords).as(Boolean.class).orElseThrow() && result.failedRecordCount() > 0) {
            var logger = runContext.logger();
            logger.error("Response show {} record failed: {}", result.failedRecordCount(), result.entries());
            throw new RuntimeException(String.format("Response show %d record failed: %s", result.failedRecordCount(), result.entries()));
        }

        // Set metrics
        runContext.metric(Timer.of("duration", Duration.ofNanos(System.nanoTime() - start)));
        runContext.metric(Counter.of("record.failed", result.failedRecordCount()));
        runContext.metric(Counter.of("record.successful", records.size() - result.failedRecordCount()));
        runContext.metric(Counter.of("record.count", records.size()));

        File tempFile = writeOutputFile(runContext, result, records);
        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .failedRecordsCount(result.failedRecordCount())
            .recordCount(records.size())
            .build();
    }

    private BatchResult putRecords(RunContext runContext, List<Record> records) throws IllegalVariableEvaluationException {
        try (KinesisClient client = client(runContext)) {
            PutRecordsRequest.Builder builder = PutRecordsRequest.builder();

            var renderedStreamArn = runContext.render(streamArn).as(String.class).orElse(null);
            var renderedStreamName = runContext.render(streamName).as(String.class).orElse(null);
            if (!Strings.isNullOrEmpty(renderedStreamArn)) {
                builder.streamARN(renderedStreamArn);
            } else if (!Strings.isNullOrEmpty(renderedStreamName)) {
                builder.streamName(renderedStreamName);
            } else {
                throw new IllegalArgumentException("Either streamName or streamArn has to be set.");
            }

            List<PutRecordsRequestEntry> putRecordsRequestEntryList = records.stream()
                .map(throwFunction(record -> record.toPutRecordsRequestEntry(runContext)))
                .collect(Collectors.toList());

            return putInBatches(putRecordsRequestEntryList, batch -> client.putRecords(builder.records(batch).build()));
        }
    }

    /**
     * Sends the entries batch by batch, in input order, so the aggregated results stay aligned with the input entries.
     * A batch with rejected records does not stop the following batches: every entry gets an attempt and
     * the failures are reported once, on the aggregated result.
     */
    static BatchResult putInBatches(List<PutRecordsRequestEntry> entries, Function<List<PutRecordsRequestEntry>, PutRecordsResponse> sender) {
        List<PutRecordsResultEntry> results = new ArrayList<>(entries.size());
        int failedRecordCount = 0;
        for (List<PutRecordsRequestEntry> batch : partition(entries)) {
            PutRecordsResponse response = sender.apply(batch);
            results.addAll(response.records());
            failedRecordCount += response.failedRecordCount();
        }
        return new BatchResult(results, failedRecordCount);
    }

    /**
     * Splits the entries into consecutive batches that each fit in a single Kinesis PutRecords request:
     * at most {@link #MAX_RECORDS_PER_REQUEST} entries and {@link #MAX_BYTES_PER_REQUEST} bytes.
     * An entry that alone exceeds the byte limit is kept in its own batch and left for Kinesis to reject.
     */
    static List<List<PutRecordsRequestEntry>> partition(List<PutRecordsRequestEntry> entries) {
        List<List<PutRecordsRequestEntry>> batches = new ArrayList<>();
        List<PutRecordsRequestEntry> current = new ArrayList<>();
        long currentBytes = 0;

        for (PutRecordsRequestEntry entry : entries) {
            long entryBytes = sizeOf(entry);
            if (!current.isEmpty() && (current.size() >= MAX_RECORDS_PER_REQUEST || currentBytes + entryBytes > MAX_BYTES_PER_REQUEST)) {
                batches.add(current);
                current = new ArrayList<>();
                currentBytes = 0;
            }
            current.add(entry);
            currentBytes += entryBytes;
        }

        if (!current.isEmpty()) {
            batches.add(current);
        }
        return batches;
    }

    /**
     * Kinesis counts the data blob and the partition key towards the request size limit.
     */
    private static long sizeOf(PutRecordsRequestEntry entry) {
        return entry.data().asByteArrayUnsafe().length + (long) entry.partitionKey().getBytes(StandardCharsets.UTF_8).length;
    }

    private List<Record> getRecordList(Object records, RunContext runContext) throws IllegalVariableEvaluationException, URISyntaxException, IOException {
        if (records instanceof String) {

            URI from = new URI(runContext.render((String) records));
            if (!from.getScheme().equals("kestra")) {
                throw new IllegalArgumentException("Invalid records parameter, must be a Kestra internal storage URI, or a list of records.");
            }
            try (var inputStream = new BufferedInputStream(runContext.storage().getFile(from), FileSerde.BUFFER_SIZE)) {
                return FileSerde.readAll(inputStream, Record.class)
                    .collectList().block();
            }
        } else if (records instanceof List) {
            return MAPPER.convertValue(records, new TypeReference<>() {
            });
        }

        throw new IllegalVariableEvaluationException("Invalid records type '" + records.getClass() + "'");
    }

    private File writeOutputFile(RunContext runContext, BatchResult result, List<Record> records) throws IOException {
        // Create Output
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var stream = new FileOutputStream(tempFile)) {
            Flux.fromIterable(records)
                .zipWithIterable(
                    result.entries(), (record, response) -> OutputEntry.builder()
                        .record(record)
                        .sequenceNumber(response.sequenceNumber())
                        .shardId(response.shardId())
                        .errorCode(response.errorCode())
                        .errorMessage(response.errorMessage())
                        .build()
                )
                .doOnEach(throwConsumer(outputEntry -> FileSerde.write(stream, outputEntry.get())))
                .collectList()
                .block();
        }
        return tempFile;
    }

    /**
     * Results of all the PutRecords requests sent for one task run, in input order.
     */
    record BatchResult(List<PutRecordsResultEntry> entries, int failedRecordCount) {
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

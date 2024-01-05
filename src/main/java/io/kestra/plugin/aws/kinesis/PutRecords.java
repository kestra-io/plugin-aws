package io.kestra.plugin.aws.kinesis;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.kinesis.model.Record;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import jakarta.validation.constraints.NotNull;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
            title = "Send multiple records as maps to Amazon Kinesis Data Streams. Check the following AWS API reference for the structure of the [PutRecordsRequestEntry](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecordsRequestEntry.html) request payload.",
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "streamName: \"mystream\"",
                "records:",
                "  - data: \"user sign-in event\"",
                "    explicitHashKey: \"optional hash value overriding the partition key\"",
                "  - data: \"user sign-out event\"",
                "    partitionKey: \"user1\""
            }
        ),
        @Example(
            title = "Send multiple records from an internal storage ion file to Amazon Kinesis Data Streams.",
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "streamName: \"mystream\"",
                "records: kestra://myfile.ion"
            }
        )
    }
)
@Schema(
    title = "Send multiple records to Amazon Kinesis Data Streams."
)
public class PutRecords extends AbstractConnection implements RunnableTask<PutRecords.Output> {
    private static final ObjectMapper MAPPER = JacksonMapper.ofIon()
        .setSerializationInclusion(JsonInclude.Include.ALWAYS);

    @PluginProperty(dynamic = false)
    @NotNull
    @Schema(
        title = "Mark the task as failed when sending a record is unsuccessful.",
        description = "If true, the task will fail when any record fails to be sent."
    )
    @Builder.Default
    private boolean failOnUnsuccessfulRecords = true;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "The name of the stream to push the records.",
        description = "Make sure to set either `streamName` or `streamArn`. One of those must be provided."
    )
    private String streamName;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "The ARN of the stream to push the records.",
        description = "Make sure to set either `streamName` or `streamArn`. One of those must be provided."
    )
    private String streamArn;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "List of records (i.e., list of maps) or internal storage URI of the file that defines the records to be sent to AWS Kinesis Data Streams.",
        description = "A list of at least one record with a map including `data` and `partitionKey` properties (those two are required arguments). Check the [PutRecordsRequestEntry](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecordsRequestEntry.html) API reference for a detailed description of required fields.",
        anyOf = {String.class, Record[].class}
    )
    @NotNull
    private Object records;

    @Override
    public Output run(RunContext runContext) throws Exception {
        final long start = System.nanoTime();

        List<Record> records = getRecordList(this.records, runContext);

        PutRecordsResponse putRecordsResponse = putRecords(runContext, records);

        // Fail if failOnUnsuccessfulRecords
        if (failOnUnsuccessfulRecords && putRecordsResponse.failedRecordCount() > 0) {
            var logger = runContext.logger();
            logger.error("Response show {} record failed: {}", putRecordsResponse.failedRecordCount(), putRecordsResponse);
            throw new RuntimeException(String.format("Response show %d record failed: %s", putRecordsResponse.failedRecordCount(), putRecordsResponse));
        }

        // Set metrics
        runContext.metric(Timer.of("duration", Duration.ofNanos(System.nanoTime() - start)));
        runContext.metric(Counter.of("failedRecordCount", putRecordsResponse.failedRecordCount()));
        runContext.metric(Counter.of("successfulRecordCount", records.size() - putRecordsResponse.failedRecordCount()));
        runContext.metric(Counter.of("recordCount", records.size()));

        File tempFile = writeOutputFile(runContext, putRecordsResponse, records);
        return Output.builder()
            .uri(runContext.putTempFile(tempFile))
            .failedRecordsCount(putRecordsResponse.failedRecordCount())
            .recordCount(records.size())
            .build();
    }

    private PutRecordsResponse putRecords(RunContext runContext, List<Record> records) throws IllegalVariableEvaluationException {
        try (KinesisClient client = client(runContext)) {
            PutRecordsRequest.Builder builder = PutRecordsRequest.builder();

            if (!Strings.isNullOrEmpty(streamArn)) {
                builder.streamARN(streamArn);
            } else if (!Strings.isNullOrEmpty(streamName)) {
                builder.streamName(streamName);
            } else {
                throw new IllegalArgumentException("Either streamName or streamArn has to be set.");
            }


            List<PutRecordsRequestEntry> putRecordsRequestEntryList = records.stream()
                .map(throwFunction(record -> record.toPutRecordsRequestEntry(runContext)))
                .collect(Collectors.toList());


            builder.records(putRecordsRequestEntryList);
            return client.putRecords(builder.build());
        }
    }

    private List<Record> getRecordList(Object records, RunContext runContext) throws IllegalVariableEvaluationException, URISyntaxException, IOException {
        if (records instanceof String) {

            URI from = new URI(runContext.render((String) records));
            if (!from.getScheme().equals("kestra")) {
                throw new IllegalArgumentException("Invalid records parameter, must be a Kestra internal storage URI, or a list of records.");
            }
            try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                return Flux.create(FileSerde.reader(inputStream, Record.class), FluxSink.OverflowStrategy.BUFFER)
                    .collectList().block();
            }
        } else if (records instanceof List) {
            return MAPPER.convertValue(records, new TypeReference<>() {
            });
        }

        throw new IllegalVariableEvaluationException("Invalid records type '" + records.getClass() + "'");
    }

    private File writeOutputFile(RunContext runContext, PutRecordsResponse putRecordsResponse, List<Record> records) throws IOException {
        // Create Output
        File tempFile = runContext.tempFile(".ion").toFile();
        try (var stream = new FileOutputStream(tempFile)) {
            Flux.fromIterable(records)
                .zipWithIterable(putRecordsResponse.records(), (record, response) -> OutputEntry.builder()
                    .record(record)
                    .sequenceNumber(response.sequenceNumber())
                    .shardId(response.shardId())
                    .errorCode(response.errorCode())
                    .errorMessage(response.errorMessage())
                    .build())
                .doOnEach(throwConsumer(outputEntry -> FileSerde.write(stream, outputEntry.get())))
                .collectList()
                .block();
        }
        return tempFile;
    }

    protected KinesisClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        KinesisClientBuilder builder = KinesisClient.builder()
            .credentialsProvider(this.credentials(runContext));

        if (this.region != null) {
            builder.region(Region.of(runContext.render(this.region)));
        }
        if (this.endpointOverride != null) {
            builder.endpointOverride(URI.create(runContext.render(this.endpointOverride)));
        }

        return builder.build();
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
            title = "The number of failed records."
        )
        private int failedRecordsCount;

        @Schema(
            title = "The total number of records sent to AWS Kinesis Data Streams."
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
            title = "The sequence number for an individual record result."
        )
        private final String sequenceNumber;

        @Schema(
            title = "The shard ID for an individual record result."
        )
        private final String shardId;

        @Schema(
            title = "The error code that indicates the failure."
        )
        private final String errorCode;

        @Schema(
            title = "The error message that explains the failure."
        )
        private final String errorMessage;

        @Schema(
            title = "The original record."
        )
        private final Record record;
    }
}

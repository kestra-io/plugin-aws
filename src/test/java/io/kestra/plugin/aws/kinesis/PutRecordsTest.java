package io.kestra.plugin.aws.kinesis;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.kinesis.model.Record;

import jakarta.inject.Inject;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import software.amazon.awssdk.services.kinesis.model.*;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Testcontainers
class PutRecordsTest extends AbstractKinesisTest {
    private static final ObjectMapper MAPPER = JacksonMapper.ofIon()
        .setSerializationInclusion(JsonInclude.Include.ALWAYS);

    @Inject
    protected RunContextFactory runContextFactory;

    private static List<PutRecords.OutputEntry> getOutputEntries(PutRecords put, RunContext runContext) throws Exception {
        var output = put.run(runContext);
        List<PutRecords.OutputEntry> outputEntries;
        URI from = output.getUri();
        if (!from.getScheme().equals("kestra")) {
            throw new IllegalArgumentException("Invalid entries parameter, must be a Kestra internal storage URI, or a list of entry.");
        }
        try (var inputStream = new BufferedInputStream(runContext.storage().getFile(from), FileSerde.BUFFER_SIZE)) {
            outputEntries = FileSerde.readAll(inputStream, PutRecords.OutputEntry.class).collectList().block();
        }
        return outputEntries;
    }

    @Test
    void runMap() throws Exception {
        var runContext = runContextFactory.of();

        Record record = Record.builder()
            .explicitHashKey("5")
            .partitionKey("partitionKey")
            .data("record")
            .build();
        Record record2 = Record.builder()
            .partitionKey("partitionKey")
            .data("record 2")
            .build();
        Record record3 = Record.builder()
            .explicitHashKey("5")
            .partitionKey("partitionKey")
            .data("record 3")
            .build();
        var put = PutRecords.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .streamName(Property.ofValue(streamName))
            .records(List.of(record, record2, record3))
            .build();

        List<PutRecords.OutputEntry> outputEntries = getOutputEntries(put, runContext);
        assertThat(outputEntries, hasSize(3));
        assertThat(outputEntries.get(0).getSequenceNumber(), notNullValue());
        assertThat(outputEntries.get(0).getErrorCode(), nullValue());
        assertThat(outputEntries.get(0).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(0).getRecord(), equalTo(record));

        assertThat(outputEntries.get(1).getSequenceNumber(), notNullValue());
        assertThat(outputEntries.get(1).getErrorCode(), nullValue());
        assertThat(outputEntries.get(1).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(1).getRecord(), equalTo(record2));

        assertThat(outputEntries.get(2).getSequenceNumber(), notNullValue());
        assertThat(outputEntries.get(2).getErrorCode(), nullValue());
        assertThat(outputEntries.get(2).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(2).getRecord(), equalTo(record3));
    }

    @Test
    void runStorage() throws Exception {
        var runContext = runContextFactory.of();

        Record record = Record.builder()
            .explicitHashKey("5")
            .partitionKey("partitionKey")
            .data("record")
            .build();
        Record record2 = Record.builder()
            .partitionKey("partitionKey")
            .data("record 2")
            .build();
        Record record3 = Record.builder()
            .explicitHashKey("5")
            .partitionKey("partitionKey")
            .data("record 3")
            .build();

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var stream = new FileOutputStream(tempFile)) {
            List.of(record, record2, record3).forEach(throwConsumer(r -> FileSerde.write(stream, r)));
        }

        var put = PutRecords.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .records(runContext.storage().putFile(tempFile).toString())
            .streamName(Property.ofValue(streamName))
            .build();

        List<PutRecords.OutputEntry> outputEntries = getOutputEntries(put, runContext);

        assertThat(outputEntries, hasSize(3));
        assertThat(outputEntries.get(0).getSequenceNumber(), notNullValue());
        assertThat(outputEntries.get(0).getErrorCode(), nullValue());
        assertThat(outputEntries.get(0).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(0).getRecord(), equalTo(record));

        assertThat(outputEntries.get(1).getSequenceNumber(), notNullValue());
        assertThat(outputEntries.get(1).getErrorCode(), nullValue());
        assertThat(outputEntries.get(1).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(1).getRecord(), equalTo(record2));

        assertThat(outputEntries.get(2).getSequenceNumber(), notNullValue());
        assertThat(outputEntries.get(2).getErrorCode(), nullValue());
        assertThat(outputEntries.get(2).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(2).getRecord(), equalTo(record3));
    }

    @Test
    void runStorageUpperCase() throws Exception {
        var runContext = runContextFactory.of();

        UpperCaseRecord record = UpperCaseRecord.builder()
            .ExplicitHashKey("5")
            .PartitionKey("partitionKey")
            .Data("record")
            .build();
        UpperCaseRecord record2 = UpperCaseRecord.builder()
            .PartitionKey("partitionKey")
            .Data("record 2")
            .build();
        UpperCaseRecord record3 = UpperCaseRecord.builder()
            .ExplicitHashKey("5")
            .PartitionKey("partitionKey")
            .Data("record 3")
            .build();

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var stream = new FileOutputStream(tempFile)) {
            List.of(record, record2, record3).forEach(throwConsumer(r -> FileSerde.write(stream, r)));
        }

        var put = PutRecords.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .records(runContext.storage().putFile(tempFile).toString())
            .streamName(Property.ofValue(streamName))
            .build();

        List<PutRecords.OutputEntry> outputEntries = getOutputEntries(put, runContext);

        assertThat(outputEntries, hasSize(3));
        assertThat(outputEntries.get(0).getSequenceNumber(), notNullValue());
        assertThat(outputEntries.get(0).getErrorCode(), nullValue());
        assertThat(outputEntries.get(0).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(0).getRecord().getData(), equalTo(record.getData()));

        assertThat(outputEntries.get(1).getSequenceNumber(), notNullValue());
        assertThat(outputEntries.get(1).getErrorCode(), nullValue());
        assertThat(outputEntries.get(1).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(1).getRecord().getData(), equalTo(record2.getData()));

        assertThat(outputEntries.get(2).getSequenceNumber(), notNullValue());
        assertThat(outputEntries.get(2).getErrorCode(), nullValue());
        assertThat(outputEntries.get(2).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(2).getRecord().getData(), equalTo(record3.getData()));
    }

    /**
     * Test that user can use AWS notation in json
     */
    @Getter
    @Builder
    @EqualsAndHashCode
    private static class UpperCaseRecord {
        private String PartitionKey;
        private String ExplicitHashKey;
        private String Data;

    }
}

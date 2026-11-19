package io.kestra.plugin.aws.kinesis;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.Storage;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.kinesis.model.Record;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class PutRecordsUnitTest {
    @Inject
    protected RunContextFactory runContextFactory;

    private static PutRecordsResponse accepted(PutRecordsRequest request) {
        return PutRecordsResponse.builder()
            .records(
                request.records().stream()
                    .map(entry -> PutRecordsResultEntry.builder().sequenceNumber("seq-%s".formatted(entry.partitionKey())).shardId("shardId-000000000000").build())
                    .toList()
            )
            .failedRecordCount(0)
            .build();
    }

    private static PutRecordsResponse throttled(PutRecordsRequest request) {
        return PutRecordsResponse.builder()
            .records(
                request.records().stream()
                    .map(entry -> PutRecordsResultEntry.builder().errorCode("ProvisionedThroughputExceededException").errorMessage("slow down").build())
                    .toList()
            )
            .failedRecordCount(request.records().size())
            .build();
    }

    private static InvalidArgumentException rejected() {
        return (InvalidArgumentException) InvalidArgumentException.builder().message("request rejected").statusCode(400).build();
    }

    private static List<Record> records(int count) {
        return IntStream.range(0, count)
            .mapToObj(i -> Record.builder().partitionKey("key-%s".formatted(i)).data("record %s".formatted(i)).build())
            .toList();
    }

    private static PutRecords task(KinesisClient client, List<Record> records) throws Exception {
        var task = spy(
            PutRecords.builder()
                .id(IdUtils.create())
                .type(PutRecords.class.getName())
                .region(Property.ofValue("us-east-1"))
                .accessKeyId(Property.ofValue("test"))
                .secretKeyId(Property.ofValue("test"))
                .streamName(Property.ofValue("stream"))
                .records(records)
                .build()
        );
        doReturn(client).when(task).client(any());
        return task;
    }

    private static List<PutRecords.OutputEntry> readOutput(RunContext runContext, URI uri) throws Exception {
        try (var inputStream = new BufferedInputStream(runContext.storage().getFile(uri), FileSerde.BUFFER_SIZE)) {
            return FileSerde.readAll(inputStream, PutRecords.OutputEntry.class).collectList().block();
        }
    }

    @Test
    void laterRequestFailureKeepsTheResultsOfEarlierRequests() throws Exception {
        var runContext = runContextFactory.of();
        var client = mock(KinesisClient.class);
        var calls = new int[] { 0 };
        when(client.putRecords(any(PutRecordsRequest.class))).thenAnswer(invocation ->
        {
            if (calls[0]++ == 1) {
                throw rejected();
            }
            return accepted(invocation.getArgument(0));
        });
        var records = records(501);

        var exception = assertThrows(RunnableTaskException.class, () -> task(client, records).run(runContext));

        verify(client, times(2)).putRecords(any(PutRecordsRequest.class));
        assertThat(exception.getCause(), instanceOf(InvalidArgumentException.class));
        assertThat(exception.getMessage(), is("Sent 500 records (0 failed), their results are in the output. Resend from position 500 after fixing: request rejected"));

        var output = (PutRecords.Output) exception.getOutput();
        assertThat(output.getRecordCount(), is(500));
        assertThat(output.getFailedRecordsCount(), is(0));
        var entries = readOutput(runContext, output.getUri());
        assertThat(entries, hasSize(500));
        for (var i = 0; i < 500; i++) {
            assertThat(entries.get(i).getRecord(), equalTo(records.get(i)));
            assertThat(entries.get(i).getSequenceNumber(), is("seq-key-%s".formatted(i)));
        }
    }

    @Test
    void killDuringARetryKeepsTheResultsSentSoFar() throws Exception {
        var runContext = runContextFactory.of();
        var client = mock(KinesisClient.class);
        var calls = new int[] { 0 };
        // The second request throttles its record, then a kill interrupts the backoff before the retry.
        when(client.putRecords(any(PutRecordsRequest.class))).thenAnswer(invocation ->
        {
            if (calls[0]++ == 1) {
                Thread.currentThread().interrupt();
                return throttled(invocation.getArgument(0));
            }
            return accepted(invocation.getArgument(0));
        });

        var exception = assertThrows(RunnableTaskException.class, () -> task(client, records(501)).run(runContext));

        verify(client, times(2)).putRecords(any(PutRecordsRequest.class));
        assertThat(exception.getCause(), instanceOf(InterruptedException.class));
        var output = (PutRecords.Output) exception.getOutput();
        assertThat(output.getRecordCount(), is(501));
        assertThat(output.getFailedRecordsCount(), is(1));
        var entries = readOutput(runContext, output.getUri());
        assertThat(entries, hasSize(501));
        assertThat(entries.get(499).getSequenceNumber(), is("seq-key-499"));
        assertThat(entries.get(500).getErrorCode(), is("ProvisionedThroughputExceededException"));
    }

    @Test
    void storageFailureKeepsTheKinesisFailureAsSuppressed() throws Exception {
        var runContext = spy(runContextFactory.of());
        var storage = mock(Storage.class);
        when(storage.putFile(any(File.class))).thenThrow(new IOException("storage unavailable"));
        doReturn(storage).when(runContext).storage();
        var client = mock(KinesisClient.class);
        var calls = new int[] { 0 };
        when(client.putRecords(any(PutRecordsRequest.class))).thenAnswer(invocation ->
        {
            if (calls[0]++ == 1) {
                throw rejected();
            }
            return accepted(invocation.getArgument(0));
        });

        var exception = assertThrows(IOException.class, () -> task(client, records(501)).run(runContext));

        assertThat(exception.getMessage(), is("storage unavailable"));
        assertThat(exception.getSuppressed(), arrayWithSize(1));
        assertThat(exception.getSuppressed()[0], instanceOf(InvalidArgumentException.class));
    }

    @Test
    void firstRequestFailureIsThrownAsIs() throws Exception {
        var runContext = runContextFactory.of();
        var client = mock(KinesisClient.class);
        when(client.putRecords(any(PutRecordsRequest.class))).thenThrow(rejected());

        var exception = assertThrows(InvalidArgumentException.class, () -> task(client, records(3)).run(runContext));

        assertThat(exception.getMessage(), startsWith("request rejected"));
    }

    @Test
    void rejectedRecordsFailTheTaskButKeepTheOutput() throws Exception {
        var runContext = runContextFactory.of();
        var client = mock(KinesisClient.class);
        when(client.putRecords(any(PutRecordsRequest.class))).thenReturn(
            PutRecordsResponse.builder()
                .records(
                    PutRecordsResultEntry.builder().sequenceNumber("seq-0").shardId("shardId-000000000000").build(),
                    PutRecordsResultEntry.builder().errorCode("KMSAccessDeniedException").errorMessage("denied").build(),
                    PutRecordsResultEntry.builder().sequenceNumber("seq-2").shardId("shardId-000000000000").build()
                )
                .failedRecordCount(1)
                .build()
        );

        var exception = assertThrows(RunnableTaskException.class, () -> task(client, records(3)).run(runContext));

        assertThat(exception.getMessage(), startsWith("1 of 3 records failed"));
        var output = (PutRecords.Output) exception.getOutput();
        assertThat(output.getRecordCount(), is(3));
        assertThat(output.getFailedRecordsCount(), is(1));
        var entries = readOutput(runContext, output.getUri());
        assertThat(entries.stream().map(PutRecords.OutputEntry::getSequenceNumber).toList(), contains("seq-0", null, "seq-2"));
        assertThat(entries.get(1).getErrorCode(), is("KMSAccessDeniedException"));
    }
}

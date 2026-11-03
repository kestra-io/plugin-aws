package io.kestra.plugin.aws.dynamodb;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.serializers.FileSerde;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class BatchWriteItemTest extends AbstractDynamoDbTest {

    @Test
    void shouldBatchWriteItemsFromList() throws Exception {
        var runContext = runContextFactory.of();
        var table = "%s-list".formatted(tableName());

        var items = new ArrayList<Object>();
        for (var i = 1; i <= 30; i++) {
            items.add(Map.of(
                "id", "user-%d".formatted(i),
                "firstname", "First%d".formatted(i),
                "lastname", "Last%d".formatted(i),
                "age", i + 20
            ));
        }

        var batchWrite = BatchWriteItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .from(items)
            .build();

        createTable(runContext, batchWrite, table);

        var output = batchWrite.run(runContext);

        assertThat(output.getCount(), is(30L));

        var counter = (Counter) runContext.metrics().stream()
            .filter(m -> "dynamodb.items.written.count".equals(m.getName()))
            .findFirst()
            .orElse(null);
        assertThat(counter, is(notNullValue()));
        assertThat(counter.getValue(), is(30D));

        var scan = Scan.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var scanOutput = scan.run(runContext);
        assertThat(scanOutput.getSize(), is(30L));
    }

    @Test
    void shouldBatchWriteItemsFromFile() throws Exception {
        var runContext = runContextFactory.of();
        var table = "%s-file".formatted(tableName());

        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            for (var i = 101; i <= 130; i++) {
                FileSerde.write(output, Map.of(
                    "id", "file-user-%d".formatted(i),
                    "firstname", "FileFirst%d".formatted(i),
                    "lastname", "FileLast%d".formatted(i)
                ));
            }
        }
        var storageUri = runContext.storage().putFile(tempFile);

        var batchWrite = BatchWriteItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .from(storageUri.toString())
            .build();

        createTable(runContext, batchWrite, table);

        var output = batchWrite.run(runContext);
        assertThat(output.getCount(), is(30L));

        var get = GetItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .key(Property.ofValue(Map.of("id", "file-user-115")))
            .build();

        var getOutput = get.run(runContext);
        assertThat(getOutput.getRow(), is(notNullValue()));
        assertThat(getOutput.getRow().get("firstname"), is("FileFirst115"));
    }

    @Test
    void shouldBatchWriteWithDeleteRequest() throws Exception {
        var runContext = runContextFactory.of();
        var table = "%s-del".formatted(tableName());

        var initialWrite = BatchWriteItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .from(List.of(
                Map.of("id", "to-keep", "name", "Keep"),
                Map.of("id", "to-delete", "name", "Delete")
            ))
            .build();

        createTable(runContext, initialWrite, table);
        initialWrite.run(runContext);

        var mixedWrite = BatchWriteItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .from(List.of(
                Map.of("deleteRequest", Map.of("id", "to-delete")),
                Map.of("putRequest", Map.of("id", "wrapped-item", "name", "Wrapped")),
                Map.of("id", "new-item", "name", "New")
            ))
            .build();

        var output = mixedWrite.run(runContext);
        assertThat(output.getCount(), is(3L));

        var getDeleted = GetItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .key(Property.ofValue(Map.of("id", "to-delete")))
            .build();
        assertThat(getDeleted.run(runContext).getRow().isEmpty(), is(true));

        var getNew = GetItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .key(Property.ofValue(Map.of("id", "new-item")))
            .build();
        var newOutput = getNew.run(runContext);
        assertThat(newOutput.getRow(), is(notNullValue()));
        assertThat(newOutput.getRow().get("name"), is("New"));

        var getWrapped = GetItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .key(Property.ofValue(Map.of("id", "wrapped-item")))
            .build();
        assertThat(getWrapped.run(runContext).getRow().get("name"), is("Wrapped"));
    }

    @Test
    void shouldReportHowManyItemsWereWrittenWhenALaterBatchFails() throws Exception {
        var runContext = runContextFactory.of();
        var table = "%s-partial".formatted(tableName());
        var items = new ArrayList<Object>();
        for (var i = 1; i <= 25; i++) {
            items.add(Map.of("id", "partial-%d".formatted(i), "name", "Partial"));
        }
        items.add(Map.of("name", "missing key"));

        var batchWrite = BatchWriteItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .from(items)
            .build();
        createTable(runContext, batchWrite, table);

        var e = assertThrows(IllegalStateException.class, () -> batchWrite.run(runContext));

        assertThat(e.getMessage(), containsString("25 put and delete request(s) were applied before the failure"));
        var counter = (Counter) runContext.metrics().stream()
            .filter(m -> "dynamodb.items.written.count".equals(m.getName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("The dynamodb.items.written.count metric is missing."));
        assertThat(counter.getValue(), is(25D));
    }

    @Test
    void shouldWriteRowsFromAFileWithoutRenderingThem() throws Exception {
        var runContext = runContextFactory.of();
        var table = "%s-curly".formatted(tableName());

        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            FileSerde.write(output, Map.of("id", "curly-item", "content", "unrendered {{ text }} and {{ 1+1 }}"));
        }
        var storageUri = runContext.storage().putFile(tempFile);

        var batchWrite = BatchWriteItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .from(storageUri.toString())
            .build();

        createTable(runContext, batchWrite, table);

        var output = batchWrite.run(runContext);
        assertThat(output.getCount(), is(1L));

        var get = GetItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .key(Property.ofValue(Map.of("id", "curly-item")))
            .build();

        var getOutput = get.run(runContext);
        assertThat(getOutput.getRow(), is(notNullValue()));
        assertThat(getOutput.getRow().get("content"), is("unrendered {{ text }} and {{ 1+1 }}"));
    }

    @Test
    void shouldResendOnlyTheUnprocessedItems() throws Exception {
        var mockDynamoDb = mock(DynamoDbClient.class);
        var table = "test-retry-table";
        var first = WriteRequest.builder().putRequest(PutRequest.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build()).build();
        var second = WriteRequest.builder().putRequest(PutRequest.builder().item(Map.of("id", AttributeValue.builder().s("2").build())).build()).build();

        when(mockDynamoDb.batchWriteItem(any(BatchWriteItemRequest.class)))
            .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(Map.of(table, List.of(second))).build())
            .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(Map.of()).build());

        var applied = new AtomicLong();
        BatchWriteItem.builder().tableName(Property.ofValue(table)).build()
            .sendBatch(mockDynamoDb, table, List.of(first, second), 3, applied);

        var requests = ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(mockDynamoDb, times(2)).batchWriteItem(requests.capture());
        assertThat(requests.getAllValues().get(1).requestItems().get(table), contains(second));
        assertThat(applied.get(), is(2L));
    }

    @Test
    void shouldThrowIllegalStateExceptionWhenRetriesExceeded() {
        var mockDynamoDb = mock(DynamoDbClient.class);
        var table = "test-fail-table";

        var writeReq = WriteRequest.builder()
            .putRequest(PutRequest.builder()
                .item(Map.of("id", AttributeValue.builder().s("1").build()))
                .build())
            .build();

        var retryResponse = BatchWriteItemResponse.builder()
            .unprocessedItems(Map.of(table, List.of(writeReq)))
            .build();

        when(mockDynamoDb.batchWriteItem(any(BatchWriteItemRequest.class)))
            .thenReturn(retryResponse);

        var task = BatchWriteItem.builder()
            .tableName(Property.ofValue(table))
            .build();

        var ex = assertThrows(IllegalStateException.class, () -> task.sendBatch(mockDynamoDb, table, List.of(writeReq), 2, new AtomicLong()));
        assertThat(ex.getMessage(), containsString("DynamoDB left 1 item(s) unprocessed after 2 retries."));
    }

}

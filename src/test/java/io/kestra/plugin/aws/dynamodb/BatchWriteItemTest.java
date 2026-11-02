package io.kestra.plugin.aws.dynamodb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class BatchWriteItemTest extends AbstractDynamoDbTest {

    private void createTable(RunContext runContext, AbstractDynamoDb dynamoDb, String tableName) throws IllegalVariableEvaluationException {
        try (var dynamoDbClient = dynamoDb.client(runContext)) {
            if (!dynamoDbClient.listTables().tableNames().contains(tableName)) {
                var request = CreateTableRequest.builder()
                    .tableName(tableName)
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                    .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build())
                    .build();
                try {
                    dynamoDbClient.createTable(request);
                } catch (ResourceInUseException ignored) {
                }
            }
        }
    }

    @Test
    void shouldBatchWriteItemsFromList() throws Exception {
        var runContext = runContextFactory.of();
        var table = tableName() + "-list";

        List<Object> items = new ArrayList<>();
        for (int i = 1; i <= 30; i++) {
            items.add(Map.of(
                "id", "user-" + i,
                "firstname", "First" + i,
                "lastname", "Last" + i,
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
        var table = tableName() + "-file";

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            for (int i = 101; i <= 130; i++) {
                FileSerde.write(output, Map.of(
                    "id", "file-user-" + i,
                    "firstname", "FileFirst" + i,
                    "lastname", "FileLast" + i
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
        var table = tableName() + "-del";

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
                Map.of("id", "new-item", "name", "New")
            ))
            .build();

        var output = mixedWrite.run(runContext);
        assertThat(output.getCount(), is(2L));

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
    }

    @Test
    void shouldWriteItemWithLiteralCurlyBraces() throws Exception {
        var runContext = runContextFactory.of();
        var table = tableName() + "-curly";

        var batchWrite = BatchWriteItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(table))
            .from(List.of(
                Map.of(
                    "id", "curly-item",
                    "content", "unrendered {{ text }} and {{ 1+1 }}"
                )
            ))
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
    void shouldRetryUnprocessedItemsAndSucceed() throws Exception {
        var mockDynamoDb = mock(DynamoDbClient.class);
        var table = "test-retry-table";

        var writeReq = WriteRequest.builder()
            .putRequest(PutRequest.builder()
                .item(Map.of("id", AttributeValue.builder().s("1").build()))
                .build())
            .build();

        var retryResponse = BatchWriteItemResponse.builder()
            .unprocessedItems(Map.of(table, List.of(writeReq)))
            .build();

        var successResponse = BatchWriteItemResponse.builder()
            .unprocessedItems(Map.of())
            .build();

        when(mockDynamoDb.batchWriteItem(any(BatchWriteItemRequest.class)))
            .thenReturn(retryResponse)
            .thenReturn(successResponse);

        var task = BatchWriteItem.builder()
            .tableName(Property.ofValue(table))
            .build();

        task.sendBatch(mockDynamoDb, table, List.of(writeReq), 3);

        verify(mockDynamoDb, times(2)).batchWriteItem(any(BatchWriteItemRequest.class));
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

        var ex = assertThrows(IllegalStateException.class, () -> task.sendBatch(mockDynamoDb, table, List.of(writeReq), 2));
        assertThat(ex.getMessage(), containsString("failed: 1 item(s) remained unprocessed after 2 retries. Earlier batches were already written."));
    }

    @Test
    void shouldThrowWhenFromIsNull() {
        var runContext = runContextFactory.of();

        var task = BatchWriteItem.builder()
            .tableName(Property.ofValue("some-table"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }
}

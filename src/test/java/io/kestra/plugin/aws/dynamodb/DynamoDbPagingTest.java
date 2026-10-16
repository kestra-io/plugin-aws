package io.kestra.plugin.aws.dynamodb;

import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class DynamoDbPagingTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldReturnScanItemsFromEveryPage() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("1"));
        var client = mock(DynamoDbClient.class);
        when(client.scan(any(ScanRequest.class))).thenAnswer(invocation ->
        {
            ScanRequest request = invocation.getArgument(0);
            if (!request.hasExclusiveStartKey()) {
                return ScanResponse.builder()
                    .items(List.of(item("1")))
                    .lastEvaluatedKey(startKey)
                    .build();
            }
            assertThat(request.exclusiveStartKey(), is(startKey));
            return ScanResponse.builder()
                .items(List.of(item("2")))
                .build();
        });

        var scan = spy(
            Scan.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .build()
        );
        doReturn(client).when(scan).client(any(RunContext.class));

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows().size(), is(2));
        verify(client, times(2)).scan(any(ScanRequest.class));
    }

    @Test
    void shouldKeepReadingScanPagesWhenTheFirstPageHasNoMatches() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("1"));
        var client = mock(DynamoDbClient.class);
        when(client.scan(any(ScanRequest.class))).thenAnswer(invocation ->
        {
            ScanRequest request = invocation.getArgument(0);
            assertThat(request.limit(), nullValue());
            assertThat(request.filterExpression(), is("lastname = :lastname"));
            if (!request.hasExclusiveStartKey()) {
                return ScanResponse.builder()
                    .lastEvaluatedKey(startKey)
                    .build();
            }
            assertThat(request.exclusiveStartKey(), is(startKey));
            return ScanResponse.builder()
                .items(List.of(item("2")))
                .build();
        });

        var scan = spy(
            Scan.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .limit(Property.ofValue(2))
                .filterExpression(Property.ofValue("lastname = :lastname"))
                .expressionAttributeValues(Property.ofValue(Map.of(":lastname", "Doe")))
                .build()
        );
        doReturn(client).when(scan).client(any(RunContext.class));

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(1L));
        verify(client, times(2)).scan(any(ScanRequest.class));
    }

    @Test
    void shouldStopReadingWhenTheExecutionIsKilled() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("1"));
        var client = mock(DynamoDbClient.class);
        var scan = spy(
            Scan.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .build()
        );
        doReturn(client).when(scan).client(any(RunContext.class));
        when(client.scan(any(ScanRequest.class))).thenAnswer(invocation ->
        {
            scan.kill();
            return ScanResponse.builder()
                .items(List.of(item("1")))
                .lastEvaluatedKey(startKey)
                .build();
        });

        var thrown = assertThrows(IllegalStateException.class, () -> scan.run(runContext));

        assertThat(thrown.getMessage(), containsString("killed"));
        verify(client, times(1)).scan(any(ScanRequest.class));
        verify(client, atLeastOnce()).close();
    }

    @Test
    void shouldStopStoringWhenTheExecutionIsKilled() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("1"));
        var client = mock(DynamoDbClient.class);
        var scan = spy(
            Scan.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.STORE))
                .build()
        );
        doReturn(client).when(scan).client(any(RunContext.class));
        when(client.scan(any(ScanRequest.class))).thenAnswer(invocation ->
        {
            scan.kill();
            return ScanResponse.builder()
                .items(List.of(item("1")))
                .lastEvaluatedKey(startKey)
                .build();
        });

        var thrown = assertThrows(IllegalStateException.class, () -> scan.run(runContext));

        assertThat(thrown.getMessage(), containsString("killed"));
        verify(client, times(1)).scan(any(ScanRequest.class));
        verify(client, atLeastOnce()).close();
    }

    @Test
    void shouldReturnQueryItemsFromThePageAfterAnEmptyMatch() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("1"));
        var client = mock(DynamoDbClient.class);
        when(client.query(any(QueryRequest.class))).thenAnswer(invocation ->
        {
            QueryRequest request = invocation.getArgument(0);
            assertThat(request.keyConditionExpression(), is("id = :id"));
            assertThat(request.filterExpression(), is("lastname = :lastname"));
            if (!request.hasExclusiveStartKey()) {
                return QueryResponse.builder()
                    .lastEvaluatedKey(startKey)
                    .build();
            }
            assertThat(request.exclusiveStartKey(), is(startKey));
            return QueryResponse.builder()
                .items(List.of(item("2")))
                .build();
        });

        var query = spy(
            Query.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .keyConditionExpression(Property.ofValue("id = :id"))
                .filterExpression(Property.ofValue("lastname = :lastname"))
                .expressionAttributeValues(Property.ofValue(Map.of(":id", "1", ":lastname", "Doe")))
                .build()
        );
        doReturn(client).when(query).client(any(RunContext.class));

        var output = query.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows().size(), is(1));
        verify(client, times(2)).query(any(QueryRequest.class));
    }

    @Test
    void shouldSendTheRemainingLimitWhenScanHasNoFilter() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("1"));
        var client = mock(DynamoDbClient.class);
        when(client.scan(any(ScanRequest.class))).thenAnswer(invocation ->
        {
            ScanRequest request = invocation.getArgument(0);
            if (!request.hasExclusiveStartKey()) {
                assertThat(request.limit(), is(2));
                return ScanResponse.builder()
                    .items(List.of(item("1")))
                    .lastEvaluatedKey(startKey)
                    .build();
            }
            assertThat(request.exclusiveStartKey(), is(startKey));
            assertThat(request.limit(), is(1));
            return ScanResponse.builder()
                .items(List.of(item("2")))
                .build();
        });

        var scan = spy(
            Scan.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .limit(Property.ofValue(2))
                .build()
        );
        doReturn(client).when(scan).client(any(RunContext.class));

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(2L));
        verify(client, times(2)).scan(any(ScanRequest.class));
    }

    @Test
    void shouldNotSendLimitWhenFetchOneUsesAFilter() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("1"));
        var calls = new AtomicInteger();
        var client = mock(DynamoDbClient.class);
        when(client.scan(any(ScanRequest.class))).thenAnswer(invocation ->
        {
            ScanRequest request = invocation.getArgument(0);
            assertThat(request.limit(), nullValue());
            assertThat(request.filterExpression(), is("lastname = :lastname"));
            if (calls.incrementAndGet() == 1) {
                return ScanResponse.builder()
                    .items(List.of(item("1"), item("2")))
                    .lastEvaluatedKey(startKey)
                    .build();
            }
            return ScanResponse.builder()
                .items(List.of(item("3")))
                .build();
        });

        var scan = spy(
            Scan.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.FETCH_ONE))
                .filterExpression(Property.ofValue("lastname = :lastname"))
                .expressionAttributeValues(Property.ofValue(Map.of(":lastname", "Doe")))
                .build()
        );
        doReturn(client).when(scan).client(any(RunContext.class));

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRow(), is(notNullValue()));
        assertThat(output.getRows(), is(nullValue()));
        verify(client, times(1)).scan(any(ScanRequest.class));
    }

    @Test
    void shouldTrimFilteredScanMatchesWithoutReadingAnotherPage() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("3"));
        var calls = new AtomicInteger();
        var client = mock(DynamoDbClient.class);
        when(client.scan(any(ScanRequest.class))).thenAnswer(invocation ->
        {
            ScanRequest request = invocation.getArgument(0);
            assertThat(request.limit(), nullValue());
            if (calls.incrementAndGet() == 1) {
                return ScanResponse.builder()
                    .items(List.of(item("1"), item("2"), item("3")))
                    .lastEvaluatedKey(startKey)
                    .build();
            }
            return ScanResponse.builder().build();
        });

        var scan = spy(
            Scan.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .limit(Property.ofValue(2))
                .filterExpression(Property.ofValue("lastname = :lastname"))
                .expressionAttributeValues(Property.ofValue(Map.of(":lastname", "Doe")))
                .build()
        );
        doReturn(client).when(scan).client(any(RunContext.class));

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows().size(), is(2));
        verify(client, times(1)).scan(any(ScanRequest.class));
    }

    @Test
    void shouldStoreEachScanPageAsAFile() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("1"));
        var client = mock(DynamoDbClient.class);
        when(client.scan(any(ScanRequest.class))).thenAnswer(invocation ->
        {
            ScanRequest request = invocation.getArgument(0);
            if (!request.hasExclusiveStartKey()) {
                return ScanResponse.builder()
                    .items(List.of(item("1")))
                    .lastEvaluatedKey(startKey)
                    .build();
            }
            assertThat(request.exclusiveStartKey(), is(startKey));
            return ScanResponse.builder()
                .items(List.of(item("2")))
                .build();
        });

        var scan = spy(
            Scan.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.STORE))
                .build()
        );
        doReturn(client).when(scan).client(any(RunContext.class));

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getUri(), is(notNullValue()));
        verify(client, times(2)).scan(any(ScanRequest.class));

        try (var input = new BufferedInputStream(runContext.storage().getFile(output.getUri()), FileSerde.BUFFER_SIZE)) {
            var rows = new ArrayList<Object>();
            FileSerde.read(input, rows::add);
            assertThat(rows.size(), is(2));
            assertThat(((Map<?, ?>) rows.get(0)).get("id"), is("1"));
            assertThat(((Map<?, ?>) rows.get(1)).get("id"), is("2"));
        }
    }

    @Test
    void shouldNotSendQueryLimitWhenAFilterIsSet() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("1"));
        var calls = new AtomicInteger();
        var client = mock(DynamoDbClient.class);
        when(client.query(any(QueryRequest.class))).thenAnswer(invocation ->
        {
            QueryRequest request = invocation.getArgument(0);
            assertThat(request.limit(), nullValue());
            assertThat(request.filterExpression(), is("lastname = :lastname"));
            if (calls.incrementAndGet() == 1) {
                return QueryResponse.builder()
                    .items(List.of(item("1"), item("2"), item("3")))
                    .lastEvaluatedKey(startKey)
                    .build();
            }
            return QueryResponse.builder().build();
        });

        var query = spy(
            Query.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .limit(Property.ofValue(2))
                .keyConditionExpression(Property.ofValue("id = :id"))
                .filterExpression(Property.ofValue("lastname = :lastname"))
                .expressionAttributeValues(Property.ofValue(Map.of(":id", "1", ":lastname", "Doe")))
                .build()
        );
        doReturn(client).when(query).client(any(RunContext.class));

        var output = query.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows().size(), is(2));
        verify(client, times(1)).query(any(QueryRequest.class));
    }

    @Test
    void shouldSendTheRemainingLimitWhenQueryHasNoFilter() throws Exception {
        var runContext = runContextFactory.of();
        var startKey = Map.of("id", AttributeValue.fromS("1"));
        var client = mock(DynamoDbClient.class);
        when(client.query(any(QueryRequest.class))).thenAnswer(invocation ->
        {
            QueryRequest request = invocation.getArgument(0);
            if (!request.hasExclusiveStartKey()) {
                assertThat(request.limit(), is(2));
                return QueryResponse.builder()
                    .items(List.of(item("1")))
                    .lastEvaluatedKey(startKey)
                    .build();
            }
            assertThat(request.exclusiveStartKey(), is(startKey));
            assertThat(request.limit(), is(1));
            return QueryResponse.builder()
                .items(List.of(item("2")))
                .build();
        });

        var query = spy(
            Query.builder()
                .tableName(Property.ofValue("persons"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .limit(Property.ofValue(2))
                .keyConditionExpression(Property.ofValue("id = :id"))
                .expressionAttributeValues(Property.ofValue(Map.of(":id", "1")))
                .build()
        );
        doReturn(client).when(query).client(any(RunContext.class));

        var output = query.run(runContext);

        assertThat(output.getSize(), is(2L));
        verify(client, times(2)).query(any(QueryRequest.class));
    }

    private static Map<String, AttributeValue> item(String id) {
        return Map.of("id", AttributeValue.fromS(id));
    }
}

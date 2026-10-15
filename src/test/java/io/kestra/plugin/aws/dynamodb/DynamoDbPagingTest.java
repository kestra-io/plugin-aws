package io.kestra.plugin.aws.dynamodb;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

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
            assertThat(request.limit(), is(2));
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

    private static Map<String, AttributeValue> item(String id) {
        return Map.of("id", AttributeValue.fromS(id));
    }
}

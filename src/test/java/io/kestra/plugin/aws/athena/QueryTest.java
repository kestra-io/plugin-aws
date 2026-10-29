package io.kestra.plugin.aws.athena;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class QueryTest {
    @Inject
    protected RunContextFactory runContextFactory;

    private GetQueryExecutionResponse succeededExecution() {
        return GetQueryExecutionResponse.builder()
            .queryExecution(
                QueryExecution.builder()
                    .status(QueryExecutionStatus.builder().state(QueryExecutionState.SUCCEEDED).build())
                    .statistics(QueryExecutionStatistics.builder().dataScannedInBytes(10L).build())
                    .build()
            )
            .build();
    }

    @SuppressWarnings("unchecked")
    @Test
    void runFetch_mockedAthena() throws Exception {
        RunContext runContext = runContextFactory.of();

        AthenaClient client = mock(AthenaClient.class);

        when(client.startQueryExecution(any(StartQueryExecutionRequest.class)))
            .thenReturn(StartQueryExecutionResponse.builder().queryExecutionId("query-123").build());

        when(client.getQueryExecution(any(GetQueryExecutionRequest.class)))
            .thenReturn(succeededExecution());

        when(client.getQueryResults(any(GetQueryResultsRequest.class)))
            .thenReturn(
                GetQueryResultsResponse.builder()
                    .resultSet(
                        ResultSet.builder()
                            .resultSetMetadata(
                                ResultSetMetadata.builder()
                                    .columnInfo(
                                        ColumnInfo.builder().name("id").type("int").build(),
                                        ColumnInfo.builder().name("name").type("string").build()
                                    )
                                    .build()
                            )
                            .rows(
                                Row.builder()
                                    .data(Datum.builder().varCharValue("1").build(), Datum.builder().varCharValue("foo").build())
                                    .build()
                            )
                            .build()
                    )
                    .build()
            );

        Query task = spy(
            Query.builder()
                .id("athena_store_test")
                .type(Query.class.getName())
                .database(Property.ofValue("db"))
                .outputLocation(Property.ofValue("s3://dummy"))
                .query(Property.ofValue("select 1"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .skipHeader(Property.ofValue(false))
                .build()
        );

        doReturn(client).when(task).athenaClient(any());

        Query.QueryOutput output = task.run(runContext);

        assertThat(output.getRows(), hasSize(1));
        Map<String, Object> row = (Map<String, Object>) output.getRows().get(0);
        assertThat(row.get("id"), is(1));
        assertThat(row.get("name"), is("foo"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void runFetch_sqlNullsAreReturnedAsNull() throws Exception {
        var runContext = runContextFactory.of();

        var client = mock(AthenaClient.class);

        when(client.startQueryExecution(any(StartQueryExecutionRequest.class)))
            .thenReturn(StartQueryExecutionResponse.builder().queryExecutionId("query-nulls").build());

        when(client.getQueryExecution(any(GetQueryExecutionRequest.class)))
            .thenReturn(succeededExecution());

        var nullDatum = Datum.builder().build();

        when(client.getQueryResults(any(GetQueryResultsRequest.class)))
            .thenReturn(
                GetQueryResultsResponse.builder()
                    .resultSet(
                        ResultSet.builder()
                            .resultSetMetadata(
                                ResultSetMetadata.builder()
                                    .columnInfo(
                                        ColumnInfo.builder().name("id").type("int").build(),
                                        ColumnInfo.builder().name("big").type("bigint").build(),
                                        ColumnInfo.builder().name("ratio").type("double").build(),
                                        ColumnInfo.builder().name("amount").type("decimal").build(),
                                        ColumnInfo.builder().name("active").type("boolean").build(),
                                        ColumnInfo.builder().name("day").type("date").build(),
                                        ColumnInfo.builder().name("ts").type("timestamp").build(),
                                        ColumnInfo.builder().name("name").type("varchar").build()
                                    )
                                    .build()
                            )
                            .rows(
                                Row.builder()
                                    .data(
                                        Datum.builder().varCharValue("1").build(),
                                        nullDatum, nullDatum, nullDatum, nullDatum, nullDatum, nullDatum, nullDatum
                                    )
                                    .build(),
                                Row.builder()
                                    .data(
                                        nullDatum,
                                        Datum.builder().varCharValue("42").build(),
                                        Datum.builder().varCharValue("1.5").build(),
                                        Datum.builder().varCharValue("10.25").build(),
                                        Datum.builder().varCharValue("true").build(),
                                        Datum.builder().varCharValue("2026-10-28").build(),
                                        Datum.builder().varCharValue("2026-10-28 10:15:30.000000").build(),
                                        Datum.builder().varCharValue("foo").build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .build()
            );

        var task = spy(
            Query.builder()
                .id(IdUtils.create())
                .type(Query.class.getName())
                .database(Property.ofValue("db"))
                .outputLocation(Property.ofValue("s3://dummy"))
                .query(Property.ofValue("select * from t"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .skipHeader(Property.ofValue(false))
                .build()
        );

        doReturn(client).when(task).athenaClient(any());

        var output = task.run(runContext);

        assertThat(output.getRows(), hasSize(2));

        var nullRow = (Map<String, Object>) output.getRows().get(0);
        assertThat(nullRow.get("id"), is(1));
        for (var column : List.of("big", "ratio", "amount", "active", "day", "ts", "name")) {
            assertThat(column, nullRow.containsKey(column), is(true));
            assertThat(column, nullRow.get(column), nullValue());
        }

        var valueRow = (Map<String, Object>) output.getRows().get(1);
        assertThat(valueRow.containsKey("id"), is(true));
        assertThat(valueRow.get("id"), nullValue());
        assertThat(valueRow.get("big"), is(42L));
        assertThat(valueRow.get("active"), is(true));
        assertThat(valueRow.get("name"), is("foo"));
    }

    // Regression for #867: default-precision timestamp(3) and no-fraction values must parse, not only 6-digit ones.
    @SuppressWarnings("unchecked")
    @Test
    void runFetch_timestampWithDefaultPrecision() throws Exception {
        var runContext = runContextFactory.of();

        var client = mock(AthenaClient.class);

        when(client.startQueryExecution(any(StartQueryExecutionRequest.class)))
            .thenReturn(StartQueryExecutionResponse.builder().queryExecutionId("query-ts").build());

        when(client.getQueryExecution(any(GetQueryExecutionRequest.class)))
            .thenReturn(succeededExecution());

        when(client.getQueryResults(any(GetQueryResultsRequest.class)))
            .thenReturn(
                GetQueryResultsResponse.builder()
                    .resultSet(
                        ResultSet.builder()
                            .resultSetMetadata(
                                ResultSetMetadata.builder()
                                    .columnInfo(ColumnInfo.builder().name("ts").type("timestamp").build())
                                    .build()
                            )
                            .rows(
                                // Athena's default timestamp(3): three fractional digits
                                Row.builder().data(Datum.builder().varCharValue("2026-10-28 10:15:30.123").build()).build(),
                                // a value with no fractional part
                                Row.builder().data(Datum.builder().varCharValue("2026-10-28 10:15:30").build()).build(),
                                // a 6-digit value still parses
                                Row.builder().data(Datum.builder().varCharValue("2026-10-28 10:15:30.123456").build()).build()
                            )
                            .build()
                    )
                    .build()
            );

        var task = spy(
            Query.builder()
                .id(IdUtils.create())
                .type(Query.class.getName())
                .database(Property.ofValue("db"))
                .outputLocation(Property.ofValue("s3://dummy"))
                .query(Property.ofValue("select ts from t"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .skipHeader(Property.ofValue(false))
                .build()
        );

        doReturn(client).when(task).athenaClient(any());

        var output = task.run(runContext);

        assertThat(output.getRows(), hasSize(3));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("ts"), is(LocalDateTime.of(2026, 10, 28, 10, 15, 30, 123_000_000)));
        assertThat(((Map<String, Object>) output.getRows().get(1)).get("ts"), is(LocalDateTime.of(2026, 10, 28, 10, 15, 30)));
        assertThat(((Map<String, Object>) output.getRows().get(2)).get("ts"), is(LocalDateTime.of(2026, 10, 28, 10, 15, 30, 123_456_000)));
    }

    // Regression: rows beyond the first Athena page must not be silently dropped.
    @SuppressWarnings("unchecked")
    @Test
    void runFetch_paginatesAcrossMultiplePages() throws Exception {
        RunContext runContext = runContextFactory.of();

        AthenaClient client = mock(AthenaClient.class);

        when(client.startQueryExecution(any(StartQueryExecutionRequest.class)))
            .thenReturn(StartQueryExecutionResponse.builder().queryExecutionId("query-page").build());

        when(client.getQueryExecution(any(GetQueryExecutionRequest.class)))
            .thenReturn(succeededExecution());

        var page1 = GetQueryResultsResponse.builder()
            .nextToken("page2token")
            .resultSet(
                ResultSet.builder()
                    .resultSetMetadata(
                        ResultSetMetadata.builder()
                            .columnInfo(ColumnInfo.builder().name("id").type("int").build())
                            .build()
                    )
                    .rows(
                        // row 0 is the Athena header, skipHeader=true will drop it
                        Row.builder().data(Datum.builder().varCharValue("id").build()).build(),
                        Row.builder().data(Datum.builder().varCharValue("1").build()).build()
                    )
                    .build()
            )
            .build();

        var page2 = GetQueryResultsResponse.builder()
            .resultSet(
                ResultSet.builder()
                    .resultSetMetadata(
                        ResultSetMetadata.builder()
                            .columnInfo(ColumnInfo.builder().name("id").type("int").build())
                            .build()
                    )
                    .rows(
                        Row.builder().data(Datum.builder().varCharValue("2").build()).build()
                    )
                    .build()
            )
            .build();

        // null token returns first page, "page2token" returns second page
        when(client.getQueryResults(any(GetQueryResultsRequest.class)))
            .thenAnswer(inv ->
            {
                GetQueryResultsRequest req = inv.getArgument(0);
                return "page2token".equals(req.nextToken()) ? page2 : page1;
            });

        Query task = spy(
            Query.builder()
                .id("athena_pagination_test")
                .type(Query.class.getName())
                .database(Property.ofValue("db"))
                .outputLocation(Property.ofValue("s3://dummy"))
                .query(Property.ofValue("select id from t"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .skipHeader(Property.ofValue(true))
                .build()
        );

        doReturn(client).when(task).athenaClient(any());

        Query.QueryOutput output = task.run(runContext);

        // header skipped from page 1, no header on page 2
        assertThat(output.getRows(), hasSize(2));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("id"), is(1));
        assertThat(((Map<String, Object>) output.getRows().get(1)).get("id"), is(2));
    }

    // Regression: STORE mode must stream ALL pages, not just the first one.
    @SuppressWarnings("unchecked")
    @Test
    void runStore_paginatesAcrossMultiplePages() throws Exception {
        RunContext runContext = runContextFactory.of();

        AthenaClient client = mock(AthenaClient.class);

        when(client.startQueryExecution(any(StartQueryExecutionRequest.class)))
            .thenReturn(StartQueryExecutionResponse.builder().queryExecutionId("query-store-page").build());

        when(client.getQueryExecution(any(GetQueryExecutionRequest.class)))
            .thenReturn(succeededExecution());

        var page1 = GetQueryResultsResponse.builder()
            .nextToken("store-page2token")
            .resultSet(
                ResultSet.builder()
                    .resultSetMetadata(
                        ResultSetMetadata.builder()
                            .columnInfo(ColumnInfo.builder().name("id").type("int").build())
                            .build()
                    )
                    .rows(
                        // row 0 is the Athena header, skipHeader=true will drop it
                        Row.builder().data(Datum.builder().varCharValue("id").build()).build(),
                        Row.builder().data(Datum.builder().varCharValue("10").build()).build()
                    )
                    .build()
            )
            .build();

        var page2 = GetQueryResultsResponse.builder()
            .resultSet(
                ResultSet.builder()
                    .resultSetMetadata(
                        ResultSetMetadata.builder()
                            .columnInfo(ColumnInfo.builder().name("id").type("int").build())
                            .build()
                    )
                    .rows(
                        Row.builder().data(Datum.builder().varCharValue("20").build()).build()
                    )
                    .build()
            )
            .build();

        when(client.getQueryResults(any(GetQueryResultsRequest.class)))
            .thenAnswer(inv ->
            {
                GetQueryResultsRequest req = inv.getArgument(0);
                return "store-page2token".equals(req.nextToken()) ? page2 : page1;
            });

        Query task = spy(
            Query.builder()
                .id("athena_store_pagination_test")
                .type(Query.class.getName())
                .database(Property.ofValue("db"))
                .outputLocation(Property.ofValue("s3://dummy"))
                .query(Property.ofValue("select id from t"))
                .fetchType(Property.ofValue(FetchType.STORE))
                .skipHeader(Property.ofValue(true))
                .build()
        );

        doReturn(client).when(task).athenaClient(any());

        Query.QueryOutput output = task.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getUri(), notNullValue());

        // Read the ION file and verify both rows are present.
        try (InputStream is = runContext.storage().getFile(output.getUri())) {
            List<Map<String, Object>> rows = Flux.from(FileSerde.readAll(is, Map.class))
                .map(m -> (Map<String, Object>) m)
                .collectList()
                .block();
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get("id"), is(10));
            assertThat(rows.get(1).get("id"), is(20));
        }
    }
}

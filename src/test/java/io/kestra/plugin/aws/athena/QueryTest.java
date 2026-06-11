package io.kestra.plugin.aws.athena;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
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
            .thenAnswer(inv -> {
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
}

package io.kestra.plugin.aws.athena;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class QueryTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void runFetch_mockedAthena() throws Exception {
        RunContext runContext = runContextFactory.of();

        AthenaClient client = mock(AthenaClient.class);

        when(client.startQueryExecution(any(StartQueryExecutionRequest.class)))
            .thenReturn(
                StartQueryExecutionResponse.builder()
                    .queryExecutionId("query-123")
                    .build()
            );

        when(client.getQueryExecution(any(GetQueryExecutionRequest.class)))
            .thenReturn(
                GetQueryExecutionResponse.builder()
                    .queryExecution(
                        QueryExecution.builder()
                            .status(
                                QueryExecutionStatus.builder()
                                    .state(QueryExecutionState.SUCCEEDED)
                                    .build()
                            )
                            .statistics(
                                QueryExecutionStatistics.builder()
                                    .dataScannedInBytes(10L)
                                    .build()
                            )
                            .build()
                    )
                    .build()
            );

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
                                    .data(
                                        Datum.builder().varCharValue("1").build(),
                                        Datum.builder().varCharValue("foo").build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .build()
            );

        Query task = spy(Query.builder()
            .id("test")
            .type(Query.class.getName())
            .database(Property.ofValue("db"))
            .outputLocation(Property.ofValue("s3://dummy"))
            .query(Property.ofValue("select 1"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .skipHeader(Property.ofValue(false))
            .build());

        doReturn(client).when(task).athenaClient(any());

        Query.QueryOutput output = task.run(runContext);

        assertThat(output.getRows(), hasSize(1));
        Map<String, Object> row = (Map<String, Object>) output.getRows().get(0);
        assertThat(row.get("id"), is(1));
        assertThat(row.get("name"), is("foo"));
    }
}

package io.kestra.plugin.aws.athena;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

/**
 * This Query task is built with the Athena SDK, more info can be found here: https://docs.aws.amazon.com/athena/latest/ug/code-samples.html.
 * A JDBC driver also exists.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query an Athena table.",
    description = """
        The query will wait for completion, except if fetchMode is set to `NONE`, and will output converted rows.
        Row conversion is based on the types listed [here](https://docs.aws.amazon.com/athena/latest/ug/data-types.html). 
        Complex data types like array, map and struct will be converted to a string."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = {
                """
                id: aws_athena_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.aws.athena.Query
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    database: my_database
                    outputLocation: s3://some-s3-bucket
                    query: |
                      select * from cloudfront_logs limit 10
                """
            }
        )
    }
)
public class Query extends AbstractConnection implements RunnableTask<Query.QueryOutput> {
    @Schema(title = "Athena catalog.")
    @PluginProperty(dynamic = true)
    private String catalog;

    @Schema(title = "Athena database.")
    @NotNull
    @PluginProperty(dynamic = true)
    private String database;

    @Schema(
        title = "Athena output location.",
        description = "The query results will be stored in this output location. Must be an existing S3 bucket."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String outputLocation;

    @Schema(title = "Athena SQL query.")
    @NotNull
    @PluginProperty(dynamic = true)
    private String query;

    @Schema(
        title = "The way you want to store the data.",
        description = "FETCH_ONE outputs the first row, "
            + "FETCH outputs all the rows, "
            + "STORE stores all rows in a file, "
            + "NONE does nothing — in this case, the task submits the query without waiting for its completion."
    )
    @NotNull
    @PluginProperty
    @Builder.Default
    private FetchType fetchType = FetchType.STORE;

    @Schema(title = "Whether to skip the first row which is usually the header.")
    @NotNull
    @PluginProperty
    @Builder.Default
    private boolean skipHeader = true;


    private static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    @Override
    public QueryOutput run(RunContext runContext) throws Exception {
        // The QueryExecutionContext allows us to set the database.
        var queryExecutionContext = QueryExecutionContext.builder()
            .catalog(catalog != null ? runContext.render(catalog) : null)
            .database(runContext.render(database))
            .build();

        // The result configuration specifies where the results of the query should go.
        var resultConfiguration = ResultConfiguration.builder()
            .outputLocation(runContext.render(outputLocation))
            .build();

        var startQueryExecutionRequest = StartQueryExecutionRequest.builder()
            .queryString(runContext.render(query))
            .queryExecutionContext(queryExecutionContext)
            .resultConfiguration(resultConfiguration)
            .build();

        try (var client = client(runContext)) {
            var startQueryExecution = client.startQueryExecution(startQueryExecutionRequest);
            runContext.logger().info("Query created with Athena execution identifier {}", startQueryExecution.queryExecutionId());
            if (fetchType == FetchType.NONE) {
                return QueryOutput.builder().queryExecutionId(startQueryExecution.queryExecutionId()).build();
            }

            var statistics = waitForQueryToComplete(client, startQueryExecution.queryExecutionId());
            if (statistics != null) {
                if (statistics.dataScannedInBytes() != null) {
                    runContext.metric(Counter.of("data.scanned.bytes", statistics.dataScannedInBytes()));
                }

                if (statistics.engineExecutionTimeInMillis() != null) {
                    runContext.metric(Counter.of("engine.execution.duration", statistics.engineExecutionTimeInMillis()));
                }

                if (statistics.queryPlanningTimeInMillis() != null) {
                    runContext.metric(Counter.of("query.planning.duration", statistics.queryPlanningTimeInMillis()));
                }

                if (statistics.queryQueueTimeInMillis() != null) {
                    runContext.metric(Counter.of("query.queue.duration", statistics.queryQueueTimeInMillis()));
                }

                if (statistics.serviceProcessingTimeInMillis() != null) {
                    runContext.metric(Counter.of("service.processing.duration", statistics.serviceProcessingTimeInMillis()));
                }

                if (statistics.totalExecutionTimeInMillis() != null) {
                    runContext.metric(Counter.of("total.execution.duration", statistics.totalExecutionTimeInMillis()));
                }
            }

            var getQueryResult = GetQueryResultsRequest.builder()
                .queryExecutionId(startQueryExecution.queryExecutionId())
                .build();
            var getQueryResultsResults = client.getQueryResults(getQueryResult);
            List<Row> results = getQueryResultsResults.resultSet().rows();
            if (skipHeader && results != null && !results.isEmpty()) {
                // we skip the first row, this is usually needed as by default Athena returns the header as the first row
                results = results.subList(1, results.size());
            }

            if (results != null) {
                runContext.metric(Counter.of("total.rows", results.size()));
            }

            List<ColumnInfo> columnInfo = getQueryResultsResults.resultSet().resultSetMetadata().columnInfo();
            QueryOutput output = null;
            if (fetchType == FetchType.FETCH_ONE) {
                Map<String, Object> row = fetchOne(columnInfo, results);
                output = QueryOutput.builder().row(row).size(row == null ? 0L : 1L).build();
            }
            else if (fetchType == FetchType.FETCH) {
                List<Object> rows = fetch(columnInfo, results);
                output = QueryOutput.builder().rows(rows).size((long) rows.size()).build();
            }
            else if (fetchType == FetchType.STORE) {
                Pair<URI, Long> pair = store(columnInfo, results, runContext);
                output = QueryOutput.builder().uri(pair.getLeft()).size(pair.getRight()).build();
            }

            if (output != null) {
                runContext.metric(Counter.of("fetch.rows", output.getSize()));
            }
            return output;
        }
    }

    private AthenaClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, AthenaClient.builder()).build();
    }

    public QueryExecutionStatistics waitForQueryToComplete(AthenaClient client, String queryExecutionId) throws InterruptedException {
        var getQueryExecutionRequest = GetQueryExecutionRequest.builder()
            .queryExecutionId(queryExecutionId)
            .build();

        QueryExecution queryExecution;
        do {
            var getQueryExecution = client.getQueryExecution(getQueryExecutionRequest);
            queryExecution = getQueryExecution.queryExecution();
            switch (queryExecution.status().state()) {
                case FAILED -> throw new RuntimeException("Amazon Athena query failed to run with error message: " +
                    getQueryExecution.queryExecution().status().stateChangeReason());
                case CANCELLED -> throw new RuntimeException("Amazon Athena query was cancelled.");
                case UNKNOWN_TO_SDK_VERSION -> throw new RuntimeException("Amazon Athena failed for an unknown reason.");
                case SUCCEEDED -> {}
                default -> Thread.sleep(500);
            }
        }
        while (queryExecution.status().state() != QueryExecutionState.SUCCEEDED);

        return queryExecution != null ? queryExecution.statistics() : null;
    }

    private Map<String, Object> fetchOne(List<ColumnInfo> columnInfo, List<Row> results) {
        if (results == null || results.isEmpty()) {
            return null;
        }

        Row row = results.get(0);
        return map(columnInfo, row);
    }

    private List<Object> fetch(List<ColumnInfo> columnInfo, List<Row> results) {
        if (results == null || results.isEmpty()) {
            return Collections.emptyList();
        }

        return results.stream().map(row -> (Object) map(columnInfo, row)).toList();
    }

    private Pair<URI, Long> store(List<ColumnInfo> columnInfo, List<Row> results, RunContext runContext) throws IOException {
        if (results == null || results.isEmpty()) {
            return Pair.of(null, 0L);
        }

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            Long count = FileSerde.writeAll(output, Flux.fromIterable(results)).block();
            return Pair.of(
                runContext.storage().putFile(tempFile),
                count
            );
        }
    }

    private Map<String, Object> map(List<ColumnInfo> columnInfo, Row row) {
        if (!row.hasData()) {
            return null;
        }

        Map<String, Object> data = new HashMap<>();
        for (int i = 0; i < columnInfo.size(); i++) {
            data.put(columnInfo.get(i).name(), mapCell(columnInfo.get(i), row.data().get(i)));
        }
        return data;
    }

    private Object mapCell(ColumnInfo columnInfo, Datum datum) {
        // We try our best to convert the result to a precise type as all data comes as a varchar.
        // See https://docs.aws.amazon.com/athena/latest/ug/data-types.html for the list of supported types.
        return switch (columnInfo.type()) {
            case "boolean" -> Boolean.valueOf(datum.varCharValue());
            case "tinyint", "smallint", "int", "integer" -> Integer.valueOf(datum.varCharValue());
            case "bigint" -> Long.valueOf(datum.varCharValue());
            case "double" -> Double.valueOf(datum.varCharValue());
            case "float" -> Float.valueOf(datum.varCharValue());
            case "decimal" -> new BigDecimal(datum.varCharValue());
            case "date" -> LocalDate.parse(datum.varCharValue(), dateFormatter);
            case "timestamp" -> LocalDateTime.parse(datum.varCharValue(), timestampFormatter);
            // default correspond to the types char, varchar, string, binary, array, map, struct
            default -> datum.varCharValue();
        };
    }

    @Builder
    @Getter
    public static class QueryOutput implements Output {

        @Schema(title = "The query execution identifier.")
        private String queryExecutionId;

        @Schema(
            title = "List containing the fetched data.",
            description = "Only populated if `fetchType=FETCH`."
        )
        private List<Object> rows;

        @Schema(
            title = "Map containing the first row of fetched data.",
            description = "Only populated if `fetchType=FETCH_ONE`."
        )
        private Map<String, Object> row;

        @Schema(
            title = "The URI of stored data.",
            description = "Only populated if `fetchType=STORE`."
        )
        private URI uri;

        @Schema(
            title = "The size of the fetched rows."
        )
        private Long size;
    }
}

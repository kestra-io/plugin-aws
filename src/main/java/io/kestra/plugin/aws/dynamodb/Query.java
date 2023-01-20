package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query items from a table."
)
@Plugin(
    examples = {
        @Example(
            title = "Query items of a table.",
            code = {
                "tableName: \"persons\"",
                "keyConditionExpression: id = :id",
                "expressionAttributeValues:",
                "  :id: \"1\""
            }
        ),
        @Example(
            title = "Query items of a table with a filter expression.",
            code = {
                "tableName: \"persons\"",
                "keyConditionExpression: id = :id",
                "expressionAttributeValues:",
                "  :id: \"1\"",
                "  :lastname: \"Doe\""
            }
        )
    }
)
public class Query extends AbstractDynamoDb implements RunnableTask<FetchOutput> {

    @Schema(
        title = "The way you want to store the data",
        description = "FETCH_ONE output the first row, "
            + "FETCH output all the rows, "
            + "STORE store all rows in a file, "
            + "NONE do nothing."
    )
    @Builder.Default
    @PluginProperty
    private FetchType fetchType = FetchType.STORE;

    @Schema(
        title = "Maximum numbers of returned results."
    )
    @PluginProperty
    private Integer limit;

    @Schema(
        title = "Query key condition expression."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String keyConditionExpression;

    @Schema(
        title = "Query expression attributes.",
        description = "Query expression attributes. It's a map of string -> object."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Map<String, Object> expressionAttributeValues;

    @Schema(
        title = "Query filter expression.",
        description = "Query filter expression. When used, 'expressionAttributeValues' must also be used."
    )
    @PluginProperty(dynamic = true)
    private String filterExpression;



    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        try (var dynamoDb = client(runContext)) {
            var queryBuilder = QueryRequest.builder()
                .tableName(runContext.render(this.getTableName()))
                .keyConditionExpression(runContext.render(keyConditionExpression))
                .expressionAttributeValues(DynamoDbUtils.valueMapFrom(expressionAttributeValues));

            if(limit != null) {
                queryBuilder.limit(limit);
            }
            if(filterExpression != null){
                queryBuilder.filterExpression(runContext.render(filterExpression));
            }


            var query = queryBuilder.build();
            var items = dynamoDb.query(query).items();
            var outputBuilder = FetchOutput.builder();
            switch (fetchType) {
                case FETCH:
                    Pair<List<Object>, Long> fetch = this.fetch(items);
                    outputBuilder
                        .rows(fetch.getLeft())
                        .size(fetch.getRight());
                    break;

                case FETCH_ONE:
                    var o = this.fetchOne(items);

                    outputBuilder
                        .row(o)
                        .size(o != null ? 1L : 0L);
                    break;

                case STORE:
                    Pair<URI, Long> store = this.store(runContext, items);
                    outputBuilder
                        .uri(store.getLeft())
                        .size(store.getRight());
                    break;
            }

            var output = outputBuilder.build();

            runContext.metric(Counter.of(
                "records", output.getSize(),
                "tableName", getTableName()
            ));

            return output;
        }
    }

    private Pair<URI, Long> store(RunContext runContext, List<Map<String, AttributeValue>> items) throws IOException {
        File tempFile = runContext.tempFile(".ion").toFile();
        AtomicLong count = new AtomicLong();

        try (var output = new FileOutputStream(tempFile)) {
            items.forEach(throwConsumer(attributes -> {
                count.incrementAndGet();
                FileSerde.write(output, DynamoDbUtils.objectMapFrom(attributes));
            }));
        }

        return Pair.of(
            runContext.putTempFile(tempFile),
            count.get()
        );
    }

    private Pair<List<Object>, Long> fetch(List<Map<String, AttributeValue>> items) {
        List<Object> result = new ArrayList<>();
        AtomicLong count = new AtomicLong();

        items.forEach(throwConsumer(attributes -> {
            count.incrementAndGet();
            result.add(DynamoDbUtils.objectMapFrom(attributes));
        }));

        return Pair.of(result, count.get());
    }


    private Map<String, Object> fetchOne(List<Map<String, AttributeValue>> items) {
        return items.stream()
            .findFirst()
            .map(attributes -> DynamoDbUtils.objectMapFrom(attributes))
            .orElse(null);
    }
}

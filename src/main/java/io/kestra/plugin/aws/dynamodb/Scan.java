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
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Scan items from a table."
)
@Plugin(
    examples = {
        @Example(
            title = "Scan all items of a table.",
            code = {
                "tableName: \"persons\""
            }
        ),
        @Example(
            title = "Scan items of a table with a filter expression.",
            code = {
                "tableName: \"persons\"",
                "filterExpression: \"lastname = :lastname\"",
                "expressionAttributeValues:",
                "  :lastname: \"Doe\""
            }
        )
    }
)
public class Scan  extends AbstractDynamoDb implements RunnableTask<FetchOutput> {

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
        title = "Scan filter expression.",
        description = "Scan filter expression. When used, 'expressionAttributeValues' must also be used."
    )
    @PluginProperty(dynamic = true)
    private String filterExpression;

    @Schema(
        title = "Scan expression attributes.",
        description = "Scan expression attributes. It's a map of string -> object."
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> expressionAttributeValues;


    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        try (var dynamoDb = client(runContext)) {
            var scanBuilder = ScanRequest.builder()
                .tableName(runContext.render(this.getTableName()));

            if(limit != null) {
                scanBuilder.limit(limit);
            }
            if(filterExpression != null){
                if(expressionAttributeValues == null){
                    throw new IllegalArgumentException("'expressionAttributeValues' must be set when 'expressionAttributeValues' is set");
                }
                scanBuilder.filterExpression(runContext.render(filterExpression));
                scanBuilder.expressionAttributeValues(valueMapFrom(expressionAttributeValues));
            }


            var scan = scanBuilder.build();
            var items = dynamoDb.scan(scan).items();
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
                FileSerde.write(output, objectMapFrom(attributes));
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
            result.add(objectMapFrom(attributes));
        }));

        return Pair.of(result, count.get());
    }


    private Map<String, Object> fetchOne(List<Map<String, AttributeValue>> items) {
        return items.stream()
            .findFirst()
            .map(attributes -> objectMapFrom(attributes))
            .orElse(null);
    }
}

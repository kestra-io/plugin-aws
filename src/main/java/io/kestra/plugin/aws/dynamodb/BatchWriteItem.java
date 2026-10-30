package io.kestra.plugin.aws.dynamodb;

import java.io.BufferedInputStream;
import java.net.URI;
import java.util.*;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Batch write items from a list into a DynamoDB table",
            full = true,
            code = """
                id: aws_dynamodb_batch_write_list
                namespace: company.team

                tasks:
                  - id: batch_write
                    type: io.kestra.plugin.aws.dynamodb.BatchWriteItem
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    tableName: "persons"
                    items:
                      - id: "1"
                        firstname: "John"
                        lastname: "Doe"
                      - id: "2"
                        firstname: "Jane"
                        lastname: "Smith"
                """
        ),
        @Example(
            title = "Batch write items from Kestra internal storage (e.g. from an upstream query or file task)",
            full = true,
            code = """
                id: aws_dynamodb_batch_write_file
                namespace: company.team

                tasks:
                  - id: batch_write
                    type: io.kestra.plugin.aws.dynamodb.BatchWriteItem
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    tableName: "customers"
                    from: "{{ outputs.extract_customers.uri }}"
                """
        )
    },
    metrics = {
        @Metric(
            name = "dynamodb.items.written.count",
            type = Counter.TYPE,
            unit = "items",
            description = "The total number of items written to DynamoDB."
        )
    }
)
@Schema(
    title = "Batch write items to DynamoDB",
    description = "Puts or deletes multiple items in DynamoDB tables using BatchWriteItem (chunks of 25). Supports inputs from a list or an internal storage file."
)
public class BatchWriteItem extends AbstractDynamoDb implements RunnableTask<BatchWriteItem.Output> {
    public static final int BATCH_LIMIT = 25;

    @Schema(
        title = "Source file URI from Kestra internal storage containing items to insert"
    )
    @PluginProperty(group = "source")
    private Property<String> from;

    @Schema(
        title = "List of items to write in map or JSON format"
    )
    @PluginProperty(group = "source")
    private Property<List<Object>> items;

    @Builder.Default
    @Schema(
        title = "Max retries for unprocessed items",
        description = "Maximum number of retry attempts when DynamoDB returns unprocessed items due to throughput throttling."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> maxRetries = Property.ofValue(5);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rTableName = runContext.render(this.tableName).as(String.class).orElseThrow(() -> missingProperty("tableName"));
        var rMaxRetries = runContext.render(this.maxRetries).as(Integer.class).orElse(5);

        boolean hasFrom = this.from != null;
        boolean hasItems = this.items != null;

        if (!hasFrom && !hasItems) {
            throw new IllegalArgumentException("Either 'items' or 'from' must be set.");
        }
        if (hasFrom && hasItems) {
            throw new IllegalArgumentException("Cannot set both 'items' and 'from'.");
        }

        long totalWritten = 0;
        try (var dynamoDb = client(runContext)) {
            var currentBatch = new ArrayList<WriteRequest>();

            if (hasFrom) {
                var rFrom = runContext.render(this.from).as(String.class).orElseThrow(() -> missingProperty("from"));
                var fromUri = new URI(rFrom);
                if (!"kestra".equals(fromUri.getScheme())) {
                    throw new IllegalArgumentException("Invalid 'from' parameter, must be a Kestra internal storage URI ('kestra://...').");
                }
                try (var inputStream = new BufferedInputStream(runContext.storage().getFile(fromUri), FileSerde.BUFFER_SIZE)) {
                    for (var obj : FileSerde.readAll(inputStream).toIterable()) {
                        currentBatch.add(toWriteRequest(runContext, obj));
                        if (currentBatch.size() == BATCH_LIMIT) {
                            sendBatch(dynamoDb, rTableName, currentBatch, rMaxRetries);
                            totalWritten += currentBatch.size();
                            currentBatch.clear();
                        }
                    }
                }
            } else {
                var rItems = runContext.render(this.items).asList(Object.class);
                for (var obj : rItems) {
                    currentBatch.add(toWriteRequest(runContext, obj));
                    if (currentBatch.size() == BATCH_LIMIT) {
                        sendBatch(dynamoDb, rTableName, currentBatch, rMaxRetries);
                        totalWritten += currentBatch.size();
                        currentBatch.clear();
                    }
                }
            }

            if (!currentBatch.isEmpty()) {
                sendBatch(dynamoDb, rTableName, currentBatch, rMaxRetries);
                totalWritten += currentBatch.size();
                currentBatch.clear();
            }
        }

        runContext.metric(Counter.of("dynamodb.items.written.count", totalWritten));

        return Output.builder()
            .count(totalWritten)
            .build();
    }

    private void sendBatch(DynamoDbClient dynamoDb, String tableName, List<WriteRequest> batch, int maxRetries) throws InterruptedException {
        var requestItems = new HashMap<String, List<WriteRequest>>();
        requestItems.put(tableName, new ArrayList<>(batch));

        int retries = 0;
        while (requestItems != null && !requestItems.isEmpty()) {
            stopIfKilled();
            var batchRequest = BatchWriteItemRequest.builder()
                .requestItems(requestItems)
                .build();
            var response = dynamoDb.batchWriteItem(batchRequest);

            if (response.hasUnprocessedItems() && !response.unprocessedItems().isEmpty()) {
                retries++;
                if (retries > maxRetries) {
                    int unprocessedCount = response.unprocessedItems().values().stream().mapToInt(List::size).sum();
                    throw new RuntimeException(
                        "DynamoDB BatchWriteItem failed: %d items remained unprocessed after %d retries.".formatted(unprocessedCount, maxRetries)
                    );
                }
                requestItems = new HashMap<>(response.unprocessedItems());
                long backoffMs = (long) (Math.pow(2, retries) * 50);
                Thread.sleep(Math.min(backoffMs, 1000));
            } else {
                requestItems = null;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private WriteRequest toWriteRequest(RunContext runContext, Object obj) throws Exception {
        Map<String, Object> fields;
        if (obj instanceof Map<?, ?> map) {
            fields = runContext.render((Map<String, Object>) map);
        } else if (obj instanceof String str) {
            fields = JacksonMapper.toMap(runContext.render(str));
        } else if (obj != null) {
            fields = JacksonMapper.toMap(obj.toString());
        } else {
            throw new IllegalArgumentException("Cannot write null item to DynamoDB.");
        }

        if (fields.containsKey("deleteRequest") && fields.get("deleteRequest") instanceof Map<?, ?> del) {
            return WriteRequest.builder()
                .deleteRequest(DeleteRequest.builder()
                    .key(valueMapFrom((Map<String, Object>) del))
                    .build())
                .build();
        }
        if (fields.containsKey("putRequest") && fields.get("putRequest") instanceof Map<?, ?> put) {
            return WriteRequest.builder()
                .putRequest(PutRequest.builder()
                    .item(valueMapFrom((Map<String, Object>) put))
                    .build())
                .build();
        }

        return WriteRequest.builder()
            .putRequest(PutRequest.builder()
                .item(valueMapFrom(fields))
                .build())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of items successfully written")
        private final long count;
    }
}


package io.kestra.plugin.aws.dynamodb;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import static io.kestra.core.utils.Rethrow.throwConsumer;

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
                    from:
                      - id: "1"
                        firstname: "John"
                        lastname: "Doe"
                      - id: "2"
                        firstname: "Jane"
                        lastname: "Smith"
                """
        ),
        @Example(
            title = "Batch write items with explicit putRequest and deleteRequest wrappers",
            full = true,
            code = """
                id: aws_dynamodb_batch_write_puts_and_deletes
                namespace: company.team

                tasks:
                  - id: batch_write
                    type: io.kestra.plugin.aws.dynamodb.BatchWriteItem
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    tableName: "persons"
                    from:
                      - putRequest:
                          id: "1"
                          firstname: "John"
                      - deleteRequest:
                          id: "2"
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
    description = "Puts or deletes multiple items in DynamoDB tables using BatchWriteItem (chunks of 25). Supports inputs from a list of maps, a single map, a JSON string, or an internal storage URI via `Data.From`. Items default to put requests, or can specify `putRequest` or `deleteRequest` wrappers. Note: DynamoDB rejects batches containing duplicate keys in the same batch request."
)
public class BatchWriteItem extends AbstractDynamoDb implements RunnableTask<BatchWriteItem.Output>, Data.From {
    public static final int BATCH_LIMIT = 25;

    @NotNull
    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION,
        anyOf = { String.class, List.class, Map.class }
    )
    @PluginProperty(group = "main")
    private Object from;

    @Builder.Default
    @Min(0)
    @Max(20)
    @Schema(
        title = "Max retries for unprocessed items",
        description = "Maximum number of retry attempts when DynamoDB returns unprocessed items due to throughput throttling. Defaults to 5."
    )
    @PluginProperty(group = "reliability")
    private Property<Integer> maxRetries = Property.ofValue(5);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rTableName = runContext.render(this.tableName).as(String.class).orElseThrow(() -> missingProperty("tableName"));
        var rMaxRetries = runContext.render(this.maxRetries).as(Integer.class).orElse(5);

        if (this.from == null) {
            throw new IllegalArgumentException("The 'from' property cannot be null.");
        }

        var totalWritten = new AtomicLong();
        try (var dynamoDb = client(runContext)) {
            Data.from(this.from)
                .read(runContext)
                .map(this::toWriteRequest)
                .buffer(BATCH_LIMIT)
                .doOnNext(throwConsumer(batch -> {
                    sendBatch(dynamoDb, rTableName, batch, rMaxRetries);
                    totalWritten.addAndGet(batch.size());
                }))
                .blockLast();
        }

        runContext.metric(Counter.of("dynamodb.items.written.count", totalWritten.get()));

        return Output.builder()
            .count(totalWritten.get())
            .build();
    }

    void sendBatch(DynamoDbClient dynamoDb, String tableName, List<WriteRequest> batch, int maxRetries) throws InterruptedException {
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
                    throw new IllegalStateException(
                        "DynamoDB BatchWriteItem failed: %d item(s) remained unprocessed after %d retries. Earlier batches were already written. Check table throughput capacity or reduce batch volume before re-running."
                            .formatted(unprocessedCount, maxRetries)
                    );
                }
                requestItems = new HashMap<>(response.unprocessedItems());
                long maxBackoffMs = Math.min(100L * (1L << retries), 5000L);
                long jitteredBackoffMs = ThreadLocalRandom.current().nextLong(maxBackoffMs / 2, maxBackoffMs + 1);
                Thread.sleep(jitteredBackoffMs);
            } else {
                requestItems = null;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private WriteRequest toWriteRequest(Map<String, Object> fields) {
        if (fields == null) {
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

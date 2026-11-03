package io.kestra.plugin.aws.dynamodb;

import java.util.List;
import java.util.Map;
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
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

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
            title = "Load the items read by a Scan into another table",
            full = true,
            code = """
                id: aws_dynamodb_batch_write_file
                namespace: company.team

                tasks:
                  - id: extract_customers
                    type: io.kestra.plugin.aws.dynamodb.Scan
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    tableName: "customers_staging"

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
            description = "The total number of put and delete requests applied to DynamoDB."
        )
    }
)
@Schema(
    title = "Batch write items to DynamoDB",
    description = """
        Puts or deletes items with BatchWriteItem, 25 per request.
        `from` takes a list of items, a single item, or a `kestra://` file such as the output of a `Scan` with `fetchType: STORE`.
        An item is written as a put, or wrapped in `putRequest` or `deleteRequest` (with the key) to choose explicitly.
        Two requests for the same key in one batch of 25 fail that batch, after the earlier batches are written."""
)
public class BatchWriteItem extends AbstractDynamoDb implements RunnableTask<BatchWriteItem.Output>, Data.From {
    private static final int BATCH_LIMIT = 25;
    private static final int DEFAULT_MAX_RETRIES = 5;

    @NotNull
    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION,
        anyOf = { String.class, List.class, Map.class }
    )
    @PluginProperty(group = "main")
    private Object from;

    @Builder.Default
    @Schema(
        title = "Max retries for unprocessed items",
        description = """
            Retries when DynamoDB returns unprocessed items because of throttling, between 0 and 20.
            Defaults to 5."""
    )
    @PluginProperty(group = "reliability")
    private Property<@Min(0) @Max(20) Integer> maxRetries = Property.ofValue(DEFAULT_MAX_RETRIES);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rTableName = runContext.render(this.tableName).as(String.class).orElseThrow(() -> missingProperty("tableName"));
        var rMaxRetries = runContext.render(this.maxRetries).as(Integer.class).orElse(DEFAULT_MAX_RETRIES);

        var totalWritten = new AtomicLong();
        try (var dynamoDb = client(runContext)) {
            onKill(runContext, dynamoDb::close);
            Data.from(this.from)
                .read(runContext)
                .map(this::toWriteRequest)
                .buffer(BATCH_LIMIT)
                .doOnNext(throwConsumer(batch -> sendBatch(dynamoDb, rTableName, batch, rMaxRetries, totalWritten)))
                .blockLast();
        } catch (RuntimeException e) {
            if (totalWritten.get() == 0) {
                throw e;
            }
            throw new IllegalStateException(
                "%d put and delete request(s) were applied before the failure. Puts and deletes can be replayed, so once the cause is fixed, rerunning the whole input is safe: %s".formatted(totalWritten.get(), e.getMessage()),
                e
            );
        } finally {
            runContext.metric(Counter.of("dynamodb.items.written.count", totalWritten.get(), "tableName", rTableName));
        }

        return Output.builder()
            .count(totalWritten.get())
            .build();
    }

    // package-private so the retry path can be tested with a mocked client, LocalStack never throttles
    void sendBatch(DynamoDbClient dynamoDb, String tableName, List<WriteRequest> batch, int maxRetries, AtomicLong applied) throws InterruptedException {
        var requestItems = Map.of(tableName, batch);
        for (var retries = 0; ; retries++) {
            stopIfKilled();
            var sent = requestItems.values().stream().mapToInt(List::size).sum();
            var unprocessed = dynamoDb.batchWriteItem(BatchWriteItemRequest.builder().requestItems(requestItems).build()).unprocessedItems();
            var unprocessedCount = unprocessed.values().stream().mapToInt(List::size).sum();
            applied.addAndGet(sent - unprocessedCount);
            if (unprocessed.isEmpty()) {
                return;
            }
            if (retries >= maxRetries) {
                throw new IllegalStateException(
                    "DynamoDB left %d item(s) unprocessed after %d retries. Check the table throughput capacity or raise `maxRetries`."
                        .formatted(unprocessedCount, maxRetries)
                );
            }
            requestItems = unprocessed;
            var maxBackoffMs = Math.min(100L * (1L << (retries + 1)), 5000L);
            Thread.sleep(ThreadLocalRandom.current().nextLong(maxBackoffMs / 2, maxBackoffMs + 1));
        }
    }

    @SuppressWarnings("unchecked")
    private WriteRequest toWriteRequest(Map<String, Object> fields) {
        // a wrapper is the only key of its row, so a regular item with such an attribute stays a put
        if (fields.size() == 1 && fields.get("deleteRequest") instanceof Map<?, ?> del) {
            return WriteRequest.builder()
                .deleteRequest(DeleteRequest.builder()
                    .key(valueMapFrom((Map<String, Object>) del))
                    .build())
                .build();
        }
        if (fields.size() == 1 && fields.get("putRequest") instanceof Map<?, ?> put) {
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
        @Schema(title = "Number of put and delete requests applied")
        private final long count;
    }
}

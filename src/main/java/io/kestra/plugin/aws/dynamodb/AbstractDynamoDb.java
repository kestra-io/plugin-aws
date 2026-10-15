package io.kestra.plugin.aws.dynamodb;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.tuple.Pair;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.aws.shared.AbstractConnection;
import io.kestra.plugin.aws.shared.ConnectionUtils;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDynamoDb extends AbstractConnection {
    @Schema(
        title = "Table name",
        description = "Target DynamoDB table for the operation."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> tableName;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<Runnable> killable = new AtomicReference<>();

    public void kill() {
        if (isKilled != null && isKilled.compareAndSet(false, true)) {
            try {
                var cancel = killable == null ? null : killable.get();
                if (cancel != null) {
                    cancel.run();
                }
            } catch (Exception e) {
                // kill() runs off the worker thread and must not fail that thread.
            }
        }
    }

    protected void onKill(RunContext runContext, Runnable cancel) {
        killable.set(() ->
        {
            try {
                cancel.run();
            } catch (Exception e) {
                runContext.logger().warn("Failed to close the DynamoDB client after the task was killed.", e);
            }
        });
    }

    protected void stopIfKilled() {
        if (isKilled.get()) {
            throw new IllegalStateException("Stopped reading DynamoDB because the execution was killed.");
        }
    }

    protected IllegalArgumentException missingProperty(String name) {
        return new IllegalArgumentException("Set '" + name + "'. The rendered value is empty.");
    }

    protected DynamoDbClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, DynamoDbClient.builder()).build();
    }

    protected Map<String, Object> objectMapFrom(Map<String, AttributeValue> fields) {
        Map<String, Object> row = new HashMap<>();
        for (var field : fields.entrySet()) {
            var key = field.getKey();
            var value = field.getValue();
            row.put(key, objectFrom(value));
        }
        return row;
    }

    protected Object objectFrom(AttributeValue value) {
        if (value == null || (value.nul() != null && value.hasSs())) {
            return null;
        }
        if (value.bool() != null && value.bool()) {
            return true;
        }
        if (value.hasSs()) {
            return value.ss();
        }
        if (value.hasM()) {
            return objectMapFrom(value.m());
        }

        //we may miss some cases, but it should be good for a first implementation.
        return value.s();
    }

    protected Map<String, AttributeValue> valueMapFrom(Map<String, Object> fields) {
        Map<String, AttributeValue> item = new HashMap<>();
        for (var field : fields.entrySet()) {
            var key = field.getKey();
            var value = field.getValue();
            item.put(key, objectFrom(value));
        }
        return item;
    }

    @SuppressWarnings("unchecked")
    protected AttributeValue objectFrom(Object value) {
        if (value == null) {
            return AttributeValue.fromNul(true);
        }
        if (value instanceof String) {
            return AttributeValue.fromS((String) value);
        }
        if (value instanceof Boolean) {
            return AttributeValue.fromBool((Boolean) value);
        }
        if (value instanceof List) {
            return AttributeValue.fromSs((List<String>) value);
        }
        if (value instanceof Map) {
            return AttributeValue.fromM(valueMapFrom((Map<String, Object>) value));
        }

        // in case we don't have any class we can handle, we call toString()
        return AttributeValue.fromS(value.toString());
    }

    protected List<Map<String, AttributeValue>> readPages(Integer maxResults, PageSource source) {
        var items = new ArrayList<Map<String, AttributeValue>>();
        Map<String, AttributeValue> exclusiveStartKey = null;
        do {
            stopIfKilled();
            var pageLimit = maxResults == null ? null : maxResults - items.size();
            if (pageLimit != null && pageLimit <= 0) {
                break;
            }
            var page = source.read(exclusiveStartKey, pageLimit);
            if (page.items() != null && !page.items().isEmpty()) {
                items.addAll(page.items());
            }
            if (maxResults != null && items.size() >= maxResults) {
                if (items.size() > maxResults) {
                    return new ArrayList<>(items.subList(0, maxResults));
                }
                return items;
            }
            exclusiveStartKey = page.lastEvaluatedKey();
        } while (exclusiveStartKey != null && !exclusiveStartKey.isEmpty());
        return items;
    }

    protected Integer maxResults(RunContext runContext, Property<Integer> limit, FetchType fetchType) throws IllegalVariableEvaluationException {
        var rLimit = limit == null ? null : runContext.render(limit).as(Integer.class).orElseThrow(() -> missingProperty("limit"));
        if (fetchType == FetchType.FETCH_ONE) {
            return rLimit == null ? 1 : Math.min(rLimit, 1);
        }
        return rLimit;
    }

    protected FetchOutput fetchOutputs(List<Map<String, AttributeValue>> items, FetchType fetchType, RunContext runContext) throws IOException, IllegalVariableEvaluationException {
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

        runContext.metric(
            Counter.of(
                "records", output.getSize(),
                "tableName", runContext.render(getTableName()).as(String.class).orElseThrow(() -> missingProperty("tableName"))
            )
        );

        return output;
    }

    private Pair<URI, Long> store(RunContext runContext, List<Map<String, AttributeValue>> items) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            var flux = Flux.fromIterable(items).map(attributes -> objectMapFrom(attributes));
            Long count = FileSerde.writeAll(output, flux).block();
            return Pair.of(
                runContext.storage().putFile(tempFile),
                count
            );
        }
    }

    private Pair<List<Object>, Long> fetch(List<Map<String, AttributeValue>> items) {
        List<Object> result = new ArrayList<>();
        AtomicLong count = new AtomicLong();

        items.forEach(throwConsumer(attributes ->
        {
            count.incrementAndGet();
            result.add(objectMapFrom(attributes));
        }));

        return Pair.of(result, count.get());
    }

    private Map<String, Object> fetchOne(List<Map<String, AttributeValue>> items) {
        return items.stream()
            .findFirst()
            .map(this::objectMapFrom)
            .orElse(null);
    }

    @FunctionalInterface
    protected interface PageSource {
        DynamoPage read(Map<String, AttributeValue> exclusiveStartKey, Integer pageLimit);
    }

    protected record DynamoPage(List<Map<String, AttributeValue>> items, Map<String, AttributeValue> lastEvaluatedKey) {
    }
}

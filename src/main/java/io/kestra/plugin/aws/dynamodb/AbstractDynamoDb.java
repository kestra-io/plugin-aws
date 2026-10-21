package io.kestra.plugin.aws.dynamodb;

import java.io.*;
import java.math.BigDecimal;
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
        if (value.bool() != null) {
            return value.bool();
        }
        if (value.n() != null) {
            return new BigDecimal(value.n());
        }
        if (value.hasSs()) {
            return value.ss();
        }
        if (value.hasNs()) {
            return value.ns().stream().map(BigDecimal::new).toList();
        }
        if (value.hasL()) {
            return value.l().stream().map(this::objectFrom).toList();
        }
        if (value.hasM()) {
            return objectMapFrom(value.m());
        }

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
        if (value instanceof Number number) {
            return AttributeValue.fromN(number.toString());
        }

        return AttributeValue.fromS(value.toString());
    }

    protected Integer maxResults(RunContext runContext, Property<Integer> limit, FetchType fetchType) throws IllegalVariableEvaluationException {
        var rLimit = limit == null ? null : runContext.render(limit).as(Integer.class).orElseThrow(() -> missingProperty("limit"));
        if (fetchType == FetchType.FETCH_ONE) {
            return rLimit == null ? 1 : Math.min(rLimit, 1);
        }
        return rLimit;
    }

    protected FetchOutput fetchOutputs(FetchType fetchType, Integer maxResults, RunContext runContext, PageSource source) throws IOException, IllegalVariableEvaluationException {
        var outputBuilder = FetchOutput.builder();
        switch (fetchType) {
            case FETCH -> {
                var fetched = this.fetch(collectPages(maxResults, source));
                outputBuilder
                    .rows(fetched.getLeft())
                    .size(fetched.getRight());
            }
            case FETCH_ONE -> {
                var row = this.fetchOne(collectPages(maxResults, source));
                outputBuilder
                    .row(row)
                    .size(row != null ? 1L : 0L);
            }
            case STORE -> {
                var stored = this.store(runContext, maxResults, source);
                outputBuilder
                    .uri(stored.getLeft())
                    .size(stored.getRight());
            }
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

    private List<Map<String, AttributeValue>> collectPages(Integer maxResults, PageSource source) {
        var items = new ArrayList<Map<String, AttributeValue>>();
        var cursor = new PageCursor(maxResults, source);
        for (var item = cursor.nextItem(); item != null; item = cursor.nextItem()) {
            items.add(item);
        }
        return items;
    }

    private Pair<URI, Long> store(RunContext runContext, Integer maxResults, PageSource source) throws IOException {
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        var cursor = new PageCursor(maxResults, source);
        try (var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            var rows = Flux.<Map<String, Object>> generate(sink ->
            {
                var item = cursor.nextItem();
                if (item == null) {
                    sink.complete();
                    return;
                }
                sink.next(objectMapFrom(item));
            });
            var count = FileSerde.writeAll(output, rows).block();
            return Pair.of(
                runContext.storage().putFile(tempFile),
                count == null ? 0L : count
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

    private final class PageCursor {
        private final Integer maxResults;
        private final PageSource source;
        private Map<String, AttributeValue> exclusiveStartKey;
        private List<Map<String, AttributeValue>> page = List.of();
        private int index;
        private long seen;
        private boolean started;

        private PageCursor(Integer maxResults, PageSource source) {
            this.maxResults = maxResults;
            this.source = source;
        }

        private Map<String, AttributeValue> nextItem() {
            while (index >= page.size()) {
                if (!readNextPage()) {
                    return null;
                }
            }
            seen++;
            return page.get(index++);
        }

        private boolean readNextPage() {
            if (maxResults != null && seen >= maxResults) {
                return false;
            }
            if (started && (exclusiveStartKey == null || exclusiveStartKey.isEmpty())) {
                return false;
            }
            stopIfKilled();
            var pageLimit = maxResults == null ? null : Math.toIntExact(maxResults - seen);
            if (pageLimit != null && pageLimit <= 0) {
                return false;
            }
            var response = source.read(exclusiveStartKey, pageLimit);
            started = true;
            exclusiveStartKey = response.lastEvaluatedKey();
            var items = response.items();
            if (items == null || items.isEmpty()) {
                page = List.of();
                index = 0;
                return exclusiveStartKey != null && !exclusiveStartKey.isEmpty();
            }
            if (maxResults != null && seen + items.size() > maxResults) {
                items = items.subList(0, Math.toIntExact(maxResults - seen));
            }
            page = items;
            index = 0;
            return true;
        }
    }
}

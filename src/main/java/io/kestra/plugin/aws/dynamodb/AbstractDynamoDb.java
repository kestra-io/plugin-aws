package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDynamoDb extends AbstractConnection {
    @Schema(title = "The DynamoDB table name.")
    @PluginProperty(dynamic = true)
    @NotNull
    protected String tableName;

    protected DynamoDbClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, DynamoDbClient.builder()).build();
    }

    protected Map<String, Object> objectMapFrom(Map<String, AttributeValue> fields) {
        Map<String, Object> row = new HashMap<>();
        for(var field : fields.entrySet()) {
            var key = field.getKey();
            var value = field.getValue();
            row.put(key, objectFrom(value));
        }
        return row;
    }

    protected Object objectFrom(AttributeValue value) {
        if(value == null || (value.nul() != null && value.hasSs())){
            return null;
        }
        if(value.bool() != null && value.bool()) {
            return true;
        }
        if(value.hasSs()) {
            return value.ss();
        }
        if(value.hasM()) {
            return objectMapFrom(value.m());
        }

        //we may miss some cases, but it should be good for a first implementation.
        return value.s();
    }

    protected Map<String, AttributeValue> valueMapFrom(Map<String, Object> fields) {
        Map<String, AttributeValue> item = new HashMap<>();
        for(var field : fields.entrySet()) {
            var key = field.getKey();
            var value = field.getValue();
            item.put(key, objectFrom(value));
        }
        return item;
    }

    @SuppressWarnings("unchecked")
    protected AttributeValue objectFrom(Object value) {
        if(value == null){
            return AttributeValue.fromNul(true);
        }
        if(value instanceof String) {
            return AttributeValue.fromS((String) value);
        }
        if(value instanceof Boolean) {
            return AttributeValue.fromBool((Boolean) value);
        }
        if(value instanceof List) {
            return AttributeValue.fromSs((List<String>) value);
        }
        if(value instanceof Map) {
            return AttributeValue.fromM(valueMapFrom((Map<String, Object>) value));
        }

        // in case we don't have any class we can handle, we call toString()
        return AttributeValue.fromS(value.toString());
    }

    protected FetchOutput fetchOutputs(List<Map<String, AttributeValue>> items, FetchType fetchType, RunContext runContext) throws IOException {
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

    private Pair<URI, Long> store(RunContext runContext, List<Map<String, AttributeValue>> items) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        AtomicLong count = new AtomicLong();

        try (var output = new FileOutputStream(tempFile)) {
            items.forEach(throwConsumer(attributes -> {
                count.incrementAndGet();
                FileSerde.write(output, objectMapFrom(attributes));
            }));
        }

        return Pair.of(
            runContext.storage().putFile(tempFile),
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
            .map(this::objectMapFrom)
            .orElse(null);
    }
}

package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractDynamoDb extends AbstractConnection {

    @Schema(title = "The DynamoDB table name.")
    @PluginProperty(dynamic = true)
    @NotNull
    private String tableName;

    protected DynamoDbClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        var builder = DynamoDbClient.builder()
            .httpClient(ApacheHttpClient.create())
            .credentialsProvider(this.credentials(runContext));

        if (this.region != null) {
            builder.region(Region.of(runContext.render(this.region)));
        }
        if (this.endpointOverride != null) {
            builder.endpointOverride(URI.create(runContext.render(this.endpointOverride)));
        }

        return builder.build();
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
}

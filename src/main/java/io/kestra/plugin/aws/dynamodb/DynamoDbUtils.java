package io.kestra.plugin.aws.dynamodb;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class DynamoDbUtils {
    private DynamoDbUtils() {
        //utility class pattern: prevent instantiation
    }

    static Map<String, Object> objectMapFrom(Map<String, AttributeValue> fields) {
        Map<String, Object> row = new HashMap<>();
        for(var field : fields.entrySet()) {
            var key = field.getKey();
            var value = field.getValue();
            row.put(key, valueFrom(value));
        }
        return row;
    }

    private static Object valueFrom(AttributeValue value) {
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

    static Map<String, AttributeValue> valueMapFrom(Map<String, Object> fields) {
        Map<String, AttributeValue> item = new HashMap<>();
        for(var field : fields.entrySet()) {
            var key = field.getKey();
            var value = field.getValue();
            item.put(key, valueFrom(value));
        }
        return item;
    }

    private static AttributeValue valueFrom(Object value) {
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

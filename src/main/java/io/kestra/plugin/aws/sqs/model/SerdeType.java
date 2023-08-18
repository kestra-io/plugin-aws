package io.kestra.plugin.aws.sqs.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public enum SerdeType {
    STRING,
    JSON;

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson(false);

    public Object deserialize(String message) throws IOException {
        if (this == SerdeType.JSON) {
            return OBJECT_MAPPER.readValue(message, Object.class);
        } else {
            return message;
        }
    }

    public String serialize(Object message) throws IOException {
        if (this == SerdeType.JSON) {
            return OBJECT_MAPPER.writeValueAsString(message);
        } else {
            return message.toString();
        }
    }
}

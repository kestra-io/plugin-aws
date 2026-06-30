package io.kestra.plugin.aws.s3.files;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public final class S3FilesService {

    private S3FilesService() {
    }

    static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    /**
     * Serializes an object to a compact JSON byte array.
     */
    public static byte[] toJson(Object value) throws IOException {
        return MAPPER.writeValueAsBytes(value);
    }

    /**
     * Deserializes a JSON string to the given type.
     */
    public static <T> T fromJson(String json, Class<T> type) throws IOException {
        return MAPPER.readValue(json, type);
    }

    /**
     * Lightweight wrapper around a raw HTTP response from the S3 Files API.
     *
     * @param statusCode HTTP status code
     * @param body raw response body (UTF-8 JSON)
     */
    public record Response(int statusCode, String body) {
    }
}

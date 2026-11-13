package io.kestra.plugin.aws.dynamodb;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;

import software.amazon.awssdk.services.dynamodb.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class GetItemTest extends AbstractDynamoDbTest {

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var get = GetItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .key(Property.ofValue(Map.of("id", "1")))
            .build();

        createTable(runContext, get);

        // create something to get
        try (var dynamoDbClient = get.client(runContext)) {
            Map<String, AttributeValue> item = Map.of(
                "id", AttributeValue.builder().s("1").build(),
                "firstname", AttributeValue.builder().s("John").build(),
                "lastname", AttributeValue.builder().s("Doe").build()
            );

            var putRequest = PutItemRequest.builder()
                .tableName(tableName())
                .item(item)
                .build();
            dynamoDbClient.putItem(putRequest);
        }

        var output = get.run(runContext);

        assertThat(output.getRow(), is(notNullValue()));
        assertThat(output.getRow().get("firstname"), is(equalTo("John")));
        assertThat(output.getRow().get("lastname"), is(equalTo("Doe")));
    }


    @Test
    void shouldReadBinaryAttributesFromDynamoDb() throws Exception {
        var runContext = runContextFactory.of();

        var get = GetItem.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .key(Property.ofValue(Map.of("id", "binary-872")))
            .build();

        createTable(runContext, get);

        try (var client = get.client(runContext)) {
            var binary = software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] {0, 1, 2}
            );

            var first = software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] {(byte) 0xAA}
            );

            var second = software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] {(byte) 0xBB}
            );

            client.putItem(PutItemRequest.builder()
                .tableName(tableName())
                .item(Map.of(
                    "id", AttributeValue.fromS("binary-872"),
                    "payload", AttributeValue.fromB(binary),
                    "payloadSet", AttributeValue.fromBs(
                        java.util.List.of(first, second)
                    )
                ))
                .build());
        }

        var row = get.run(runContext).getRow();

        assertThat(row, is(notNullValue()));
        assertThat(row.get("payload"), is("AAEC"));
        assertThat(row.get("payloadSet"), instanceOf(java.util.List.class));

        assertThat(
            java.util.Set.copyOf((java.util.List<?>) row.get("payloadSet")),
            is(java.util.Set.of("qg==", "uw=="))
        );
    }

}

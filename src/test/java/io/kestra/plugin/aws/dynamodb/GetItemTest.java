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
            .tableName(Property.ofValue("persons"))
            .key(Property.ofValue(Map.of("id", "get-item-1")))
            .build();

        createTable(runContext, get);

        // create something to get
        try (var dynamoDbClient = get.client(runContext)) {
            Map<String, AttributeValue> item = Map.of(
                "id", AttributeValue.builder().s("get-item-1").build(),
                "firstname", AttributeValue.builder().s("John").build(),
                "lastname", AttributeValue.builder().s("Doe").build()
            );

            var putRequest = PutItemRequest.builder()
                .tableName("persons")
                .item(item)
                .build();
            dynamoDbClient.putItem(putRequest);
        }

        var output = get.run(runContext);

        assertThat(output.getRow(), is(notNullValue()));
        assertThat(output.getRow().get("firstname"), is(equalTo("John")));
        assertThat(output.getRow().get("lastname"), is(equalTo("Doe")));
    }

}
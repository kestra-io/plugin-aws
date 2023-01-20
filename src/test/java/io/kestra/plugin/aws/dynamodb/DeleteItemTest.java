package io.kestra.plugin.aws.dynamodb;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class DeleteItemTest extends AbstractDynamoDbTest {

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var delete = DeleteItem.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString())
            .region(localstack.getRegion())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .tableName("persons")
            .keyName("id")
            .keyValue("1")
            .build();

        createTable(runContext, delete);

        // create something to delete
        try (var dynamoDbClient = delete.client(runContext)) {
            Map<String, AttributeValue> item = Map.of(
                "id",  AttributeValue.builder().s("1").build(),
                "firstname",  AttributeValue.builder().s("John").build(),
                "lastname",  AttributeValue.builder().s("Doe").build()
            );

            var putRequest = PutItemRequest.builder()
                .tableName("persons")
                .item(item)
                .build();
            dynamoDbClient.putItem(putRequest);
        }

        var output = delete.run(runContext);

        assertThat(output, is(nullValue()));
    }
}
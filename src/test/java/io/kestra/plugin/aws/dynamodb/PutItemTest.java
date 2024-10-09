package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.models.property.Property;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class PutItemTest extends AbstractDynamoDbTest {

    @Test
    void runMap() throws Exception {
        var runContext = runContextFactory.of();

        var put = PutItem.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString())
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .tableName("persons")
            .item(Map.of(
                "id", "1",
                "firstname", "John",
                "lastname", "Doe"
            ))
            .build();

        createTable(runContext, put);

        var output = put.run(runContext);

        assertThat(output, is(nullValue()));
    }

    @Test
    void runString() throws Exception {
        var runContext = runContextFactory.of();

        var put = PutItem.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString())
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .tableName("persons")
            .item("{\"id\": \"1\", \"firstname\": \"Jane\", \"lastname\": \"Doe\"}")
            .build();

        createTable(runContext, put);

        var output = put.run(runContext);

        assertThat(output, is(nullValue()));
    }
}
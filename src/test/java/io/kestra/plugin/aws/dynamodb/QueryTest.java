package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class QueryTest extends AbstractDynamoDbTest {

    @Test
    void runFetch() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .tableName("persons")
            .keyConditionExpression("id = :id")
            .expressionAttributeValues(Map.of(":id", "1"))
            .fetchType(FetchType.FETCH)
            .build();

        createTable(runContext, query);

        // create something to search
        initTable(runContext, query);

        var output = query.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows().size(), is(1));
        assertThat(output.getUri(), is(nullValue()));
    }

    @Test
    void runFetchWithExpression() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .tableName("persons")
            .keyConditionExpression("id = :id")
            .filterExpression("lastname = :lastname")
            .expressionAttributeValues(Map.of(":id", "1", ":lastname", "Doe"))
            .fetchType(FetchType.FETCH)
            .build();

        createTable(runContext, query);

        // create something to search
        initTable(runContext, query);

        var output = query.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows().size(), is(1));
        assertThat(output.getUri(), is(nullValue()));
    }

    @Test
    void runFetchMultipleExpression() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .tableName("persons")
            .keyConditionExpression("id = :id")
            .expressionAttributeValues(Map.of(":id", "1"))
            .fetchType(FetchType.FETCH)
            .build();

        createTable(runContext, query);

        // create something to search
        initTable(runContext, query);

        var output = query.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows().size(), is(1));
        assertThat(output.getUri(), is(nullValue()));
    }

    @Test
    void runFetchOne() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .tableName("persons")
            .keyConditionExpression("id = :id")
            .expressionAttributeValues(Map.of(":id", "1"))
            .fetchType(FetchType.FETCH_ONE)
            .build();

        createTable(runContext, query);

        // create something to search
        initTable(runContext, query);

        var output = query.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRow(), is(notNullValue()));
        assertThat(output.getUri(), is(nullValue()));
    }


    @Test
    void runStored() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .tableName("persons")
            .keyConditionExpression("id = :id")
            .expressionAttributeValues(Map.of(":id", "1"))
            .fetchType(FetchType.STORE)
            .build();

        createTable(runContext, query);

        // create something to search
        initTable(runContext, query);

        var output = query.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getUri(), is(notNullValue()));
    }

    private void initTable(RunContext runContext, Query query) throws IllegalVariableEvaluationException {
        try (var dynamoDbClient = query.client(runContext)) {
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

            item = Map.of(
                "id",  AttributeValue.builder().s("2").build(),
                "firstname",  AttributeValue.builder().s("Jane").build(),
                "lastname",  AttributeValue.builder().s("Doe").build()
            );
            putRequest = PutItemRequest.builder()
                .tableName("persons")
                .item(item)
                .build();
            dynamoDbClient.putItem(putRequest);

            item = Map.of(
                "id",  AttributeValue.builder().s("3").build(),
                "firstname",  AttributeValue.builder().s("Charles").build(),
                "lastname",  AttributeValue.builder().s("Baudelaire").build()
            );
            putRequest = PutItemRequest.builder()
                .tableName("persons")
                .item(item)
                .build();
            dynamoDbClient.putItem(putRequest);
        }
    }
}
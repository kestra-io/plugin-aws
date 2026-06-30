package io.kestra.plugin.aws.dynamodb;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class QueryTest extends AbstractDynamoDbTest {

    @Test
    void runFetch() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .keyConditionExpression(Property.ofValue("id = :id"))
            .expressionAttributeValues(Property.ofValue(Map.of(":id", "1")))
            .fetchType(Property.ofValue(FetchType.FETCH))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .keyConditionExpression(Property.ofValue("id = :id"))
            .filterExpression(Property.ofValue("lastname = :lastname"))
            .expressionAttributeValues(Property.ofValue(Map.of(":id", "1", ":lastname", "Doe")))
            .fetchType(Property.ofValue(FetchType.FETCH))
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
    void runFetchWithExpressionNoMatch() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .keyConditionExpression(Property.ofValue("id = :id"))
            .filterExpression(Property.ofValue("lastname = :lastname"))
            .expressionAttributeValues(Property.ofValue(Map.of(":id", "1", ":lastname", "Baudelaire")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        createTable(runContext, query);
        initTable(runContext, query);

        var output = query.run(runContext);

        assertThat(output.getSize(), is(0L));
        assertThat(output.getRows().size(), is(0));
        assertThat(output.getUri(), is(nullValue()));
    }

    @Test
    void runFetchMultipleExpression() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .keyConditionExpression(Property.ofValue("id = :id"))
            .expressionAttributeValues(Property.ofValue(Map.of(":id", "1")))
            .fetchType(Property.ofValue(FetchType.FETCH))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .keyConditionExpression(Property.ofValue("id = :id"))
            .expressionAttributeValues(Property.ofValue(Map.of(":id", "1")))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .keyConditionExpression(Property.ofValue("id = :id"))
            .expressionAttributeValues(Property.ofValue(Map.of(":id", "1")))
            .fetchType(Property.ofValue(FetchType.STORE))
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
                "id", AttributeValue.builder().s("1").build(),
                "firstname", AttributeValue.builder().s("John").build(),
                "lastname", AttributeValue.builder().s("Doe").build()
            );
            var putRequest = PutItemRequest.builder()
                .tableName(tableName())
                .item(item)
                .build();
            dynamoDbClient.putItem(putRequest);

            item = Map.of(
                "id", AttributeValue.builder().s("2").build(),
                "firstname", AttributeValue.builder().s("Jane").build(),
                "lastname", AttributeValue.builder().s("Doe").build()
            );
            putRequest = PutItemRequest.builder()
                .tableName(tableName())
                .item(item)
                .build();
            dynamoDbClient.putItem(putRequest);

            item = Map.of(
                "id", AttributeValue.builder().s("3").build(),
                "firstname", AttributeValue.builder().s("Charles").build(),
                "lastname", AttributeValue.builder().s("Baudelaire").build()
            );
            putRequest = PutItemRequest.builder()
                .tableName(tableName())
                .item(item)
                .build();
            dynamoDbClient.putItem(putRequest);
        }
    }
}

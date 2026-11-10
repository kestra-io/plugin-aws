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

    @Test
    void shouldQueryBinaryAttributesFromDynamoDb() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .keyConditionExpression(Property.ofValue("id = :id"))
            .expressionAttributeValues(Property.ofValue(Map.of(":id", "binary-query-872")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        createTable(runContext, query);

        try (var client = query.client(runContext)) {
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
                    "id", AttributeValue.fromS("binary-query-872"),
                    "payload", AttributeValue.fromB(binary),
                    "payloadSet", AttributeValue.fromBs(
                        java.util.List.of(first, second)
                    )
                ))
                .build());
        }

        var output = query.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows(), is(notNullValue()));
        assertThat(output.getRows().size(), is(1));

        var row = (Map<?, ?>) output.getRows().get(0);

        assertThat(row.get("payload"), is("AAEC"));
        assertThat(
            java.util.Set.copyOf((java.util.List<?>) row.get("payloadSet")),
            is(java.util.Set.of("qg==", "uw=="))
        );

        var storedQuery = Query.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .keyConditionExpression(Property.ofValue("id = :id"))
            .expressionAttributeValues(Property.ofValue(Map.of(":id", "binary-query-872")))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        var stored = storedQuery.run(runContext);

        assertThat(stored.getSize(), is(1L));
        assertThat(stored.getRows(), is(nullValue()));
        assertThat(stored.getUri(), is(notNullValue()));

        try (var input = new java.io.BufferedInputStream(
            runContext.storage().getFile(stored.getUri()),
            io.kestra.core.serializers.FileSerde.BUFFER_SIZE
        )) {
            var rows = new java.util.ArrayList<Object>();
            io.kestra.core.serializers.FileSerde.read(input, rows::add);

            assertThat(rows.size(), is(1));

            var storedRow = (Map<?, ?>) rows.get(0);

            assertThat(storedRow.get("payload"), is("AAEC"));
            assertThat(
                storedRow.get("payloadSet"),
                org.hamcrest.Matchers.instanceOf(java.util.List.class)
            );
            assertThat(
                java.util.Set.copyOf((java.util.List<?>) storedRow.get("payloadSet")),
                is(java.util.Set.of("qg==", "uw=="))
            );
        }

    }

}

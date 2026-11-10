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
import static org.hamcrest.Matchers.*;

class ScanTest extends AbstractDynamoDbTest {

    @Test
    void runFetch() throws Exception {
        var runContext = runContextFactory.of();

        var scan = Scan.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .filterExpression(Property.ofValue("lastname = :lastname"))
            .expressionAttributeValues(Property.ofValue(Map.of(":lastname", "Doe")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        createTable(runContext, scan);

        // create something to search
        initTable(runContext, scan);

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows().size(), is(2));
        assertThat(output.getUri(), is(nullValue()));
    }

    @Test
    void runFetchNoExpression() throws Exception {
        var runContext = runContextFactory.of();

        var scan = Scan.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        createTable(runContext, scan);

        // create something to search
        initTable(runContext, scan);

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(3L));
        assertThat(output.getRows().size(), is(3));
        assertThat(output.getUri(), is(nullValue()));
    }

    @Test
    void runFetchMultipleExpression() throws Exception {
        var runContext = runContextFactory.of();

        var scan = Scan.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .filterExpression(Property.ofValue("lastname = :lastname and firstname = :firstname"))
            .expressionAttributeValues(Property.ofValue(Map.of(":lastname", "Doe", ":firstname", "Jane")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        createTable(runContext, scan);

        // create something to search
        initTable(runContext, scan);

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows().size(), is(1));
        assertThat(output.getUri(), is(nullValue()));
    }

    @Test
    void runFetchOne() throws Exception {
        var runContext = runContextFactory.of();

        var scan = Scan.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .filterExpression(Property.ofValue("lastname = :lastname"))
            .expressionAttributeValues(Property.ofValue(Map.of(":lastname", "Doe")))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        createTable(runContext, scan);

        // create something to search
        initTable(runContext, scan);

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRow(), is(notNullValue()));
        assertThat(output.getUri(), is(nullValue()));
    }

    @Test
    void runStored() throws Exception {
        var runContext = runContextFactory.of();

        var scan = Scan.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(tableName()))
            .filterExpression(Property.ofValue("lastname = :lastname"))
            .expressionAttributeValues(Property.ofValue(Map.of(":lastname", "Doe")))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        createTable(runContext, scan);

        // create something to search
        initTable(runContext, scan);

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getUri(), is(notNullValue()));
    }

    private void initTable(RunContext runContext, Scan scan) throws IllegalVariableEvaluationException {
        try (var dynamoDbClient = scan.client(runContext)) {
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
    void shouldScanBinaryAttributesFromDynamoDb() throws Exception {
        var runContext = runContextFactory.of();
        var isolatedTable = tableName() + "-binary-fetch-872";

        var scan = Scan.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(isolatedTable))
            .filterExpression(Property.ofValue("id = :id"))
            .expressionAttributeValues(Property.ofValue(Map.of(":id", "binary-scan-872")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        createTable(runContext, scan, isolatedTable);

        try (var client = scan.client(runContext)) {
            var binary = software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] { 0, 1, 2 }
            );

            var first = software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] { (byte) 0xAA }
            );

            var second = software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] { (byte) 0xBB }
            );

            client.putItem(
                PutItemRequest.builder()
                    .tableName(isolatedTable)
                    .item(
                        Map.of(
                            "id", AttributeValue.fromS("binary-scan-872"),
                            "payload", AttributeValue.fromB(binary),
                            "payloadSet", AttributeValue.fromBs(
                                java.util.List.of(first, second)
                            )
                        )
                    )
                    .build()
            );
        }

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows(), is(notNullValue()));
        assertThat(output.getRows().size(), is(1));

        var row = (Map<?, ?>) output.getRows().get(0);

        assertThat(row.get("payload"), is("AAEC"));
        assertThat(
            row.get("payloadSet"),
            instanceOf(java.util.List.class)
        );

        assertThat(
            java.util.Set.copyOf((java.util.List<?>) row.get("payloadSet")),
            is(java.util.Set.of("qg==", "uw=="))
        );
    }

    @Test
    void shouldStoreBinaryAttributesWithoutLosingData() throws Exception {
        var runContext = runContextFactory.of();
        var isolatedTable = tableName() + "-binary-store-872";

        var scan = Scan.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .tableName(Property.ofValue(isolatedTable))
            .filterExpression(Property.ofValue("id = :id"))
            .expressionAttributeValues(Property.ofValue(Map.of(":id", "binary-store-872")))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        createTable(runContext, scan, isolatedTable);

        try (var client = scan.client(runContext)) {
            client.putItem(
                PutItemRequest.builder()
                    .tableName(isolatedTable)
                    .item(
                        Map.of(
                            "id", AttributeValue.fromS("binary-store-872"),
                            "payload", AttributeValue.fromB(
                                software.amazon.awssdk.core.SdkBytes.fromByteArray(
                                    new byte[] { 0, 1, 2 }
                                )
                            ),
                            "payloadSet", AttributeValue.fromBs(
                                java.util.List.of(
                                    software.amazon.awssdk.core.SdkBytes.fromByteArray(
                                        new byte[] { (byte) 0xAA }
                                    ),
                                    software.amazon.awssdk.core.SdkBytes.fromByteArray(
                                        new byte[] { (byte) 0xBB }
                                    )
                                )
                            )
                        )
                    )
                    .build()
            );
        }

        var output = scan.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getUri(), is(notNullValue()));

        try (
            var input = new java.io.BufferedInputStream(
                runContext.storage().getFile(output.getUri()),
                io.kestra.core.serializers.FileSerde.BUFFER_SIZE
            )
        ) {
            var storedRows = new java.util.ArrayList<Object>();
            io.kestra.core.serializers.FileSerde.read(input, storedRows::add);

            assertThat(storedRows.size(), is(1));

            var row = (Map<?, ?>) storedRows.get(0);

            assertThat(row.get("payload"), is("AAEC"));
            assertThat(
                row.get("payloadSet"),
                instanceOf(java.util.List.class)
            );
            assertThat(
                java.util.Set.copyOf((java.util.List<?>) row.get("payloadSet")),
                is(java.util.Set.of("qg==", "uw=="))
            );
        }
    }

}

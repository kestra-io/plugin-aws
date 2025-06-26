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

class ScanTest extends AbstractDynamoDbTest {

    @Test
    void runFetch() throws Exception {
        var runContext = runContextFactory.of();

        var scan = Scan.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .tableName(Property.ofValue("persons"))
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
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .tableName(Property.ofValue("persons"))
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
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .tableName(Property.ofValue("persons"))
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
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .tableName(Property.ofValue("persons"))
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
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .tableName(Property.ofValue("persons"))
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
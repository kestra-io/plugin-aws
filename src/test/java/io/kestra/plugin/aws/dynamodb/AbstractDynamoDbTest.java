package io.kestra.plugin.aws.dynamodb;

import org.testcontainers.junit.jupiter.Testcontainers;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractLocalStackTest;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.dynamodb.model.*;

@KestraTest
@Testcontainers
public abstract class AbstractDynamoDbTest extends AbstractLocalStackTest {

    @Inject
    protected RunContextFactory runContextFactory;

    void createTable(RunContext runContext, AbstractDynamoDb dynamoDb) throws IllegalVariableEvaluationException {
        try (var dynamoDbClient = dynamoDb.client(runContext)) {
            if (!dynamoDbClient.listTables().tableNames().contains("persons")) {
                var request = CreateTableRequest.builder()
                    .tableName("persons")
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                    .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build())
                    .build();
                dynamoDbClient.createTable(request);
            }
        }
    }

}

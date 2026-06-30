package io.kestra.plugin.aws.dynamodb;

import org.testcontainers.junit.jupiter.Testcontainers;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractFlociTest;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.dynamodb.model.*;

@KestraTest
@Testcontainers
public abstract class AbstractDynamoDbTest extends AbstractFlociTest {

    @Inject
    protected RunContextFactory runContextFactory;

    protected String tableName() {
        return "persons-" + getClass().getSimpleName().toLowerCase().replace("test", "");
    }

    void createTable(RunContext runContext, AbstractDynamoDb dynamoDb) throws IllegalVariableEvaluationException {
        var table = tableName();
        try (var dynamoDbClient = dynamoDb.client(runContext)) {
            if (!dynamoDbClient.listTables().tableNames().contains(table)) {
                var request = CreateTableRequest.builder()
                    .tableName(table)
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                    .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build())
                    .build();
                try {
                    dynamoDbClient.createTable(request);
                } catch (ResourceInUseException ignored) {
                    // created by a concurrent test class
                }
            }
        }
    }

}

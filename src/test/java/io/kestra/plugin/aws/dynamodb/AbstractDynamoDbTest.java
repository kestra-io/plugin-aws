package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.dynamodb.model.*;

@MicronautTest
@Testcontainers
public abstract class AbstractDynamoDbTest extends AbstractLocalStackTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

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

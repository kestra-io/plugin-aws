package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.dynamodb.model.*;

@MicronautTest
@Testcontainers
public abstract class AbstractDynamoDbTest {
    protected static LocalStackContainer localstack;

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.3.1"))
            .withServices(LocalStackContainer.Service.DYNAMODB);
        localstack.start();
    }

    @AfterAll
    static void stopLocalstack() {
        if(localstack != null) {
            localstack.stop();
        }
    }

    void createTable(RunContext runContext, AbstractDynamoDb dynamoDb) throws IllegalVariableEvaluationException {
        try (var dynamoDbClient = dynamoDb.client(runContext)) {
            if(!dynamoDbClient.listTables().tableNames().contains("persons")){
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

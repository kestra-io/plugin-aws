package io.kestra.plugin.aws.sqs;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.util.List;

@MicronautTest
@Testcontainers
public class AbstractSqsTest {
    protected static LocalStackContainer localstack;

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.3.1"))
            .withServices(LocalStackContainer.Service.SQS);
        // we must use fixed port in order the TriggerTest to work as it uses a real flow with hardcoded configuration
        localstack.setPortBindings(List.of("4566:4566"));
        localstack.start();
    }

    @AfterAll
    static void stopLocalstack() {
        if(localstack != null) {
            localstack.stop();
        }
    }

    String queueUrl() {
        return localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString() + "/000000000000/test-queue";
    }

    void createQueue(SqsClient client) {
        if(!client.listQueues().queueUrls().contains(queueUrl())) {
            client.createQueue(CreateQueueRequest.builder().queueName("test-queue").build());
        }
    }
}

package io.kestra.plugin.aws.sqs;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

@MicronautTest
@Testcontainers
public class AbstractSqsTest extends AbstractLocalStackTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    String queueUrl() {
        return localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString() + "/000000000000/test-queue";
    }

    void createQueue(SqsClient client) {
        if (!client.listQueues().queueUrls().contains(queueUrl())) {
            client.createQueue(CreateQueueRequest.builder().queueName("test-queue").build());
        }
    }
}

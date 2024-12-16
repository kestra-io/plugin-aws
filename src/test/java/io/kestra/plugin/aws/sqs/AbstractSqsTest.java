package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

@KestraTest
@Testcontainers
public class AbstractSqsTest extends AbstractLocalStackTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @BeforeEach
    void beforeEach() {
        try(SqsClient sqsClient = SqsClient
            .builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
            .region(Region.of(localstack.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
            ))
            .build()) {
            if (!sqsClient.listQueues().queueUrls().contains(queueUrl())) {
                sqsClient.createQueue(CreateQueueRequest.builder().queueName("test-queue").build());
            }
        }
    }

    String queueUrl() {
        return localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString() + "/000000000000/test-queue";
    }

}

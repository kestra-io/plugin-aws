package io.kestra.plugin.aws.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractFlociTest;

import jakarta.inject.Inject;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;

@KestraTest
@Testcontainers
public class AbstractSqsTest extends AbstractFlociTest {

    @Inject
    protected RunContextFactory runContextFactory;

    protected String queueName() {
        return "test-queue-" + getClass().getSimpleName().toLowerCase();
    }

    @BeforeEach
    void beforeEach() {
        try (
            SqsClient sqsClient = SqsClient
                .builder()
                .endpointOverride(java.net.URI.create(endpointUrl()))
                .region(Region.of(REGION))
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
                    )
                )
                .build()
        ) {
            if (!sqsClient.listQueues().queueUrls().contains(queueUrl())) {
                sqsClient.createQueue(CreateQueueRequest.builder().queueName(queueName()).build());
            } else {
                sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl()).build());
            }
        }
    }

    String queueUrl() {
        return endpointUrl() + "/000000000000/" + queueName();
    }

}

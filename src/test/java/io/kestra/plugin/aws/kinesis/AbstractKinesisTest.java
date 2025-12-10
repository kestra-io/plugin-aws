package io.kestra.plugin.aws.kinesis;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

public class AbstractKinesisTest extends AbstractLocalStackTest {
    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String STREAM_NAME = "stream";

    @BeforeEach
    void beforeEach() throws InterruptedException {
        try(KinesisClient kinesisClient = KinesisClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                localstack.getAccessKey(),
                localstack.getSecretKey()
            )))
            .region(Region.of(localstack.getRegion()))
            .endpointOverride(localstack.getEndpoint())
            .build()) {

            try {
                kinesisClient.deleteStream(DeleteStreamRequest.builder()
                    .streamName(STREAM_NAME)
                    .enforceConsumerDeletion(true)
                    .build());
                Thread.sleep(500);
            } catch (Exception ignored) {}

            kinesisClient.createStream(CreateStreamRequest.builder()
                .streamName(STREAM_NAME)
                .shardCount(1)
                .build());

            DescribeStreamResponse ds = kinesisClient.describeStream(r -> r.streamName(STREAM_NAME));
            while (ds.streamDescription().streamStatus() != StreamStatus.ACTIVE) {
                Thread.sleep(200);
                ds = kinesisClient.describeStream(r -> r.streamName(STREAM_NAME));
            }
        }
    }
}

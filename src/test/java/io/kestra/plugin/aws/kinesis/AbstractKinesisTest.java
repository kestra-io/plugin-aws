package io.kestra.plugin.aws.kinesis;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.util.UUID;

public class AbstractKinesisTest extends AbstractLocalStackTest {
    @Inject
    protected RunContextFactory runContextFactory;
    protected static String streamArn;
    protected static String streamName;

    @BeforeAll
    static void setupStream() throws InterruptedException {
        streamName = "stream-" + IdUtils.create();

        try (KinesisClient kinesisClient = KinesisClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
            ))
            .region(Region.of(localstack.getRegion()))
            .endpointOverride(localstack.getEndpoint())
            .build()) {

            kinesisClient.createStream(CreateStreamRequest.builder()
                .streamName(streamName)
                .shardCount(1)
                .build());

            DescribeStreamResponse ds;
            do {
                Thread.sleep(200);
                ds = kinesisClient.describeStream(r -> r.streamName(streamName));
            } while (ds.streamDescription().streamStatus() != StreamStatus.ACTIVE);

            streamArn = ds.streamDescription().streamARN();
        }
    }


    protected String registerConsumer() throws Exception {
        try (KinesisClient kinesis = KinesisClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                localstack.getAccessKey(),
                localstack.getSecretKey())))
            .region(Region.of(localstack.getRegion()))
            .endpointOverride(localstack.getEndpoint())
            .build()) {

            var consumerName = "kestra-" + IdUtils.create();
            RegisterStreamConsumerResponse resp = kinesis.registerStreamConsumer(r -> r.streamARN(streamArn).consumerName(consumerName));

            Thread.sleep(1000);
            return resp.consumer().consumerARN();
        }
    }
}

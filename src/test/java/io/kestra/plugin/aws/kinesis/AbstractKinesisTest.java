package io.kestra.plugin.aws.kinesis;

import org.junit.jupiter.api.BeforeEach;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.AbstractFlociTest;

import jakarta.inject.Inject;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

public class AbstractKinesisTest extends AbstractFlociTest {
    @Inject
    protected RunContextFactory runContextFactory;
    protected String streamArn;
    protected String streamName;

    @BeforeEach
    void setupStream() throws InterruptedException {
        streamName = "stream-" + IdUtils.create();

        try (
            KinesisClient kinesisClient = KinesisClient.builder()
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
                    )
                )
                .region(Region.of(REGION))
                .endpointOverride(java.net.URI.create(endpointUrl()))
                .build()
        ) {

            kinesisClient.createStream(
                CreateStreamRequest.builder()
                    .streamName(streamName)
                    .shardCount(1)
                    .build()
            );

            DescribeStreamResponse ds;
            do {
                Thread.sleep(200);
                ds = kinesisClient.describeStream(r -> r.streamName(streamName));
            } while (ds.streamDescription().streamStatus() != StreamStatus.ACTIVE);

            streamArn = ds.streamDescription().streamARN();
        }
    }

    protected String registerConsumer() throws Exception {
        try (
            KinesisClient kinesis = KinesisClient.builder()
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                            ACCESS_KEY,
                            SECRET_KEY
                        )
                    )
                )
                .region(Region.of(REGION))
                .endpointOverride(java.net.URI.create(endpointUrl()))
                .build()
        ) {

            var consumerName = "kestra-" + IdUtils.create();
            RegisterStreamConsumerResponse resp = kinesis.registerStreamConsumer(r -> r.streamARN(streamArn).consumerName(consumerName));

            Thread.sleep(1000);
            return resp.consumer().consumerARN();
        }
    }
}

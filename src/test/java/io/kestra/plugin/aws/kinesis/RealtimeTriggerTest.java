package io.kestra.plugin.aws.kinesis;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Testcontainers
class RealtimeTriggerTest {
    private static LocalStackContainer localstack;

    @Inject
    private RunContextFactory runContextFactory;

    private static KinesisAsyncClient asyncClient;

    private static String consumerArn;

    @BeforeAll
    static void init() throws Exception {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0"));
        localstack.start();

        asyncClient = KinesisAsyncClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                localstack.getAccessKey(), localstack.getSecretKey()
            )))
            .region(Region.of(localstack.getRegion()))
            .endpointOverride(localstack.getEndpoint())
            .build();

        asyncClient.createStream(CreateStreamRequest.builder()
            .streamName("stream")
            .shardCount(1)
            .build()).get();

        DescribeStreamResponse ds = asyncClient.describeStream(r -> r.streamName("stream")).get();
        while (ds.streamDescription().streamStatus() != StreamStatus.ACTIVE) {
            Thread.sleep(300);
            ds = asyncClient.describeStream(r -> r.streamName("stream")).get();
        }


        RegisterStreamConsumerResponse registerStreamConsumerResponse = asyncClient.registerStreamConsumer(
            RegisterStreamConsumerRequest.builder()
                .streamARN(ds.streamDescription().streamARN())
                .consumerName("kestra-test")
                .build()
        ).get();

        DescribeStreamConsumerResponse describeStreamConsumerResponse = asyncClient.describeStreamConsumer(
            DescribeStreamConsumerRequest.builder()
                .consumerARN(registerStreamConsumerResponse.consumer().consumerARN())
                .build()
        ).get();

        while (describeStreamConsumerResponse.consumerDescription().consumerStatus() != ConsumerStatus.ACTIVE) {
            Thread.sleep(300);
            describeStreamConsumerResponse = asyncClient.describeStreamConsumer(
                DescribeStreamConsumerRequest.builder()
                    .consumerARN(registerStreamConsumerResponse.consumer().consumerARN())
                    .build()
            ).get();
        }

        consumerArn = describeStreamConsumerResponse.consumerDescription().consumerARN();
    }

    @AfterAll
    static void cleanup() {
        if (localstack != null)
            localstack.stop();
    }

    @Test
    void testRealtimeTrigger() throws Exception {
        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id(RealtimeTriggerTest.class.getSimpleName())
            .type(RealtimeTrigger.class.getName())
            .stream(Property.ofValue("stream"))
            .consumerArn(Property.ofValue(consumerArn))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .iteratorType(Property.ofValue("TRIM_HORIZON"))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Flux<Execution> flux = Flux.from(trigger.evaluate(context.getKey(), context.getValue()));

        Thread.sleep(500);

        asyncClient.putRecord(PutRecordRequest.builder()
            .streamName("stream")
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("hello"))
            .build()).get();

        Execution execution = flux.blockFirst(java.time.Duration.ofSeconds(5));

        assertThat(execution, is(notNullValue()));
        assertThat(execution.getTrigger(), is(notNullValue()));
        assertThat(execution.getTrigger().getVariables().get("data"), is("hello"));
    }
}

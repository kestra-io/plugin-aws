package io.kestra.plugin.aws.kinesis;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Testcontainers
class TriggerTest {
    private static LocalStackContainer localstack;

    @Inject
    protected RunContextFactory runContextFactory;

    private static KinesisClient client;

    @BeforeAll
    static void init() throws Exception {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0"));
        localstack.start();

        client = KinesisClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                localstack.getAccessKey(), localstack.getSecretKey()
            )))
            .region(Region.of(localstack.getRegion()))
            .endpointOverride(localstack.getEndpoint())
            .build();

        client.createStream(CreateStreamRequest.builder()
            .streamName("stream")
            .shardCount(1)
            .build());

        DescribeStreamResponse describeStreamResponse = client.describeStream(DescribeStreamRequest.builder().streamName("stream").build());

        while (describeStreamResponse.streamDescription().streamStatus() != StreamStatus.ACTIVE) {
            Thread.sleep(500);
            describeStreamResponse = client.describeStream(
                DescribeStreamRequest.builder().streamName("stream").build());
        }
    }

    @AfterAll
    static void shutdown() {
        if (localstack != null)
            localstack.stop();
    }

    @Test
    void evaluate() throws Exception {
        for (int i = 0; i < 3; i++) {
            client.putRecord(PutRecordRequest.builder()
                .streamName("stream")
                .data(SdkBytes.fromUtf8String("msg " + i))
                .partitionKey("pk")
                .build());
        }

        var trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getName())
            .streamName(Property.ofValue("stream"))
            .iteratorType(Property.ofValue("TRIM_HORIZON"))
            .maxRecords(Property.ofValue(3))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
    }
}

package io.kestra.plugin.aws.kinesis;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.aws.AbstractLocalStackTest;
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Testcontainers
class ConsumeTest {
    private static LocalStackContainer localstack;

    @Inject
    protected RunContextFactory runContextFactory;

    @BeforeAll
    static void startLocalstack() throws Exception {
        localstack = new LocalStackContainer(DockerImageName.parse(AbstractLocalStackTest.LOCALSTACK_VERSION));
        localstack.start();

        KinesisClient client = client();
        client.createStream(CreateStreamRequest.builder()
            .streamName("stream")
            .shardCount(1)
            .build());

        DescribeStreamResponse stream = client.describeStream(DescribeStreamRequest.builder().streamName("stream").build());
        while (stream.streamDescription().streamStatus() != StreamStatus.ACTIVE) {
            stream = client.describeStream(DescribeStreamRequest.builder().streamName("stream").build());
        }
    }

    @AfterAll
    static void stopLocalstack() {
        if (localstack != null) {
            localstack.stop();
        }
    }

    private static KinesisClient client() throws IllegalVariableEvaluationException {
        return KinesisClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                localstack.getAccessKey(),
                localstack.getSecretKey()
            )))
            .region(Region.of(localstack.getRegion()))
            .endpointOverride(localstack.getEndpoint())
            .build();
    }

    private static List<Consume.ConsumedRecord> loadOutput(RunContext ctx, URI uri) throws Exception {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(ctx.storage().getFile(uri)))) {
            return FileSerde.readAll(r, Consume.ConsumedRecord.class).collectList().block();
        }
    }


    @Test
    void testConsume() throws Exception {
        var runContext = runContextFactory.of();
        KinesisClient client = client();

        for (int i = 0; i < 3; i++) {
            client.putRecord(PutRecordRequest.builder()
                .streamName("stream")
                .partitionKey("pk")
                .data(SdkBytes.fromUtf8String("msg " + i))
                .build());
        }

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .stream(Property.ofValue("stream"))
            .iteratorType(Property.ofValue("TRIM_HORIZON"))
            .maxRecords(Property.ofValue(10))
            .pollDuration(Property.ofValue(java.time.Duration.ofSeconds(1)))
            .build();

        var output = consume.run(runContext);

        assertThat(output.getRecordCount(), is(3));
        assertThat(output.getLastSequencePerShard(), aMapWithSize(1));

        List<Consume.ConsumedRecord> records = loadOutput(runContext, output.getUri());
        assertThat(records, hasSize(3));

        assertThat(records.getFirst().getData(), startsWith("msg "));
        assertThat(records.getFirst().getPartitionKey(), equalTo("pk"));
        assertThat(records.getFirst().getSequenceNumber(), notNullValue());
        assertThat(records.getFirst().getShardId(), notNullValue());
        assertThat(records.getFirst().getApproximateArrivalTimestamp(), instanceOf(Instant.class));
    }
}

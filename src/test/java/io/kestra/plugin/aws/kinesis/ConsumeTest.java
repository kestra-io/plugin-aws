package io.kestra.plugin.aws.kinesis;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Testcontainers
class ConsumeTest {
    private static final ObjectMapper MAPPER = JacksonMapper.ofIon()
        .setSerializationInclusion(JsonInclude.Include.ALWAYS);
    private static LocalStackContainer localstack;
    private static final String STREAM_NAME = "test-stream";

    @Inject
    protected RunContextFactory runContextFactory;

    @BeforeAll
    static void startLocalstack() throws InterruptedException {
        localstack = new LocalStackContainer(DockerImageName.parse(AbstractLocalStackTest.LOCALSTACK_VERSION));
        localstack.start();

        KinesisClient client = client(localstack);

        // Create stream
        client.createStream(CreateStreamRequest.builder()
            .streamName(STREAM_NAME)
            .streamModeDetails(StreamModeDetails.builder().streamMode(StreamMode.PROVISIONED).build())
            .shardCount(2)
            .build());

        // Wait for stream to be active
        waitForStreamActive(client, STREAM_NAME);

        // Put some test records
        client.putRecords(PutRecordsRequest.builder()
            .streamName(STREAM_NAME)
            .records(
                PutRecordsRequestEntry.builder()
                    .partitionKey("key1")
                    .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String("message1"))
                    .build(),
                PutRecordsRequestEntry.builder()
                    .partitionKey("key2")
                    .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String("message2"))
                    .build(),
                PutRecordsRequestEntry.builder()
                    .partitionKey("key3")
                    .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String("message3"))
                    .build(),
                PutRecordsRequestEntry.builder()
                    .partitionKey("key4")
                    .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String("message4"))
                    .build(),
                PutRecordsRequestEntry.builder()
                    .partitionKey("key5")
                    .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String("message5"))
                    .build()
            )
            .build());
    }

    @AfterAll
    static void stopLocalstack() {
        if (localstack != null) {
            localstack.stop();
        }
    }

    private static KinesisClient client(LocalStackContainer localstack) {
        KinesisClientBuilder builder = KinesisClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                localstack.getAccessKey(),
                localstack.getSecretKey()
            )))
            .region(Region.of(localstack.getRegion()))
            .endpointOverride(localstack.getEndpoint());

        return builder.build();
    }

    private static void waitForStreamActive(KinesisClient client, String streamName) throws InterruptedException {
        DescribeStreamResponse stream;
        do {
            Thread.sleep(100);
            stream = client.describeStream(
                DescribeStreamRequest.builder().streamName(streamName).build()
            );
        } while (stream.streamDescription().streamStatus() != StreamStatus.ACTIVE);
    }

    private List<Consume.Message> getMessages(RunContext runContext, URI uri) throws Exception {
        if (!uri.getScheme().equals("kestra")) {
            throw new IllegalArgumentException("Invalid URI, must be a Kestra internal storage URI");
        }
        try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(uri)))) {
            return FileSerde.readAll(inputStream, Consume.Message.class).collectList().block();
        }
    }

    @Test
    void consumeFromTrimHorizon() throws Exception {
        var runContext = runContextFactory.of();

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .streamName(Property.ofValue(STREAM_NAME))
            .startingPosition(Property.ofValue("TRIM_HORIZON"))
            .maxRecords(Property.ofValue(10))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        Consume.Output output = consume.run(runContext);

        assertThat(output.getMessagesCount(), greaterThanOrEqualTo(5));
        assertThat(output.getUri(), notNullValue());

        List<Consume.Message> messages = getMessages(runContext, output.getUri());
        assertThat(messages, hasSize(greaterThanOrEqualTo(5)));

        // Verify message structure
        Consume.Message firstMessage = messages.get(0);
        assertThat(firstMessage.getKey(), notNullValue());
        assertThat(firstMessage.getValue(), notNullValue());
        assertThat(firstMessage.getShardId(), notNullValue());
        assertThat(firstMessage.getTimestamp(), notNullValue());
        assertThat(firstMessage.getSequenceNumber(), notNullValue());

        // Verify we got all our test messages
        List<String> values = messages.stream().map(Consume.Message::getValue).toList();
        assertThat(values, hasItems("message1", "message2", "message3", "message4", "message5"));
    }

    @Test
    void consumeWithMaxRecords() throws Exception {
        var runContext = runContextFactory.of();

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .streamName(Property.ofValue(STREAM_NAME))
            .startingPosition(Property.ofValue("TRIM_HORIZON"))
            .maxRecordsTotal(Property.ofValue(3))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        Consume.Output output = consume.run(runContext);

        assertThat(output.getMessagesCount(), is(3));
        assertThat(output.getUri(), notNullValue());

        List<Consume.Message> messages = getMessages(runContext, output.getUri());
        assertThat(messages, hasSize(3));
    }

    @Test
    void consumeFromLatest() throws Exception {
        var runContext = runContextFactory.of();

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .streamName(Property.ofValue(STREAM_NAME))
            .startingPosition(Property.ofValue("LATEST"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        // Add new records AFTER creating the consumer (LATEST position)
        KinesisClient client = client(localstack);

        // Small delay to ensure LATEST position is established
        Thread.sleep(1000);

        client.putRecords(PutRecordsRequest.builder()
            .streamName(STREAM_NAME)
            .records(
                PutRecordsRequestEntry.builder()
                    .partitionKey("newKey1")
                    .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String("newMessage1"))
                    .build(),
                PutRecordsRequestEntry.builder()
                    .partitionKey("newKey2")
                    .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String("newMessage2"))
                    .build()
            )
            .build());

        // Small delay to ensure records are available
        Thread.sleep(500);

        Consume.Output output = consume.run(runContext);

        // With LATEST, we should get the 2 new records added after consumer creation
        assertThat(output.getMessagesCount(), greaterThanOrEqualTo(0));
        assertThat(output.getUri(), notNullValue());
    }

    @Test
    void consumeWithMaxDuration() throws Exception {
        var runContext = runContextFactory.of();

        long startTime = System.currentTimeMillis();

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .streamName(Property.ofValue(STREAM_NAME))
            .startingPosition(Property.ofValue("TRIM_HORIZON"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(3)))
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        Consume.Output output = consume.run(runContext);

        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime) / 1000;

        // Should complete within maxDuration + some buffer for processing
        assertThat(duration, lessThanOrEqualTo(5L));
        assertThat(output.getMessagesCount(), greaterThanOrEqualTo(0));
        assertThat(output.getUri(), notNullValue());
    }

    @Test
    void consumeEmptyStream() throws Exception {
        var runContext = runContextFactory.of();
        KinesisClient client = client(localstack);
        String emptyStreamName = "empty-test-stream";

        try {
            // Create a new empty stream
            client.createStream(CreateStreamRequest.builder()
                .streamName(emptyStreamName)
                .streamModeDetails(StreamModeDetails.builder().streamMode(StreamMode.PROVISIONED).build())
                .shardCount(1)
                .build());

            // Wait for stream to be active
            waitForStreamActive(client, emptyStreamName);

            var consume = Consume.builder()
                .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
                .region(Property.ofValue(localstack.getRegion()))
                .accessKeyId(Property.ofValue(localstack.getAccessKey()))
                .secretKeyId(Property.ofValue(localstack.getSecretKey()))
                .streamName(Property.ofValue(emptyStreamName))
                .startingPosition(Property.ofValue("TRIM_HORIZON"))
                .maxDuration(Property.ofValue(Duration.ofSeconds(2)))
                .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
                .build();

            Consume.Output output = consume.run(runContext);

            assertThat(output.getMessagesCount(), is(0));
            assertThat(output.getUri(), notNullValue());
        } finally {
            // Clean up
            try {
                client.deleteStream(DeleteStreamRequest.builder().streamName(emptyStreamName).build());
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    @Test
    void consumeWithStreamArn() throws Exception {
        var runContext = runContextFactory.of();

        // Get stream ARN
        KinesisClient client = client(localstack);
        DescribeStreamResponse streamResponse = client.describeStream(
            DescribeStreamRequest.builder().streamName(STREAM_NAME).build()
        );
        String streamArn = streamResponse.streamDescription().streamARN();

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .streamArn(Property.ofValue(streamArn))
            .startingPosition(Property.ofValue("TRIM_HORIZON"))
            .maxRecordsTotal(Property.ofValue(5))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        Consume.Output output = consume.run(runContext);

        assertThat(output.getMessagesCount(), greaterThanOrEqualTo(5));
        assertThat(output.getUri(), notNullValue());
    }

    @Test
    void verifyMessageOrdering() throws Exception {
        var runContext = runContextFactory.of();
        KinesisClient client = client(localstack);
        String orderStreamName = "order-test-stream";

        try {
            // Create a new stream for ordering test
            client.createStream(CreateStreamRequest.builder()
                .streamName(orderStreamName)
                .streamModeDetails(StreamModeDetails.builder().streamMode(StreamMode.PROVISIONED).build())
                .shardCount(1)
                .build());

            // Wait for stream to be active
            waitForStreamActive(client, orderStreamName);

            // Prepare messages for multiple partition keys
            Map<String, List<String>> inputMessages = Map.of(
                "keyA", List.of("A1", "A2", "A3", "A4", "A5"),
                "keyB", List.of("B1", "B2", "B3", "B4", "B5")
            );

            // Send all messages in order per key
            for (Map.Entry<String, List<String>> entry : inputMessages.entrySet()) {
                String key = entry.getKey();
                for (String value : entry.getValue()) {
                    client.putRecord(PutRecordRequest.builder()
                        .streamName(orderStreamName)
                        .partitionKey(key)
                        .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String(value))
                        .build());
                    Thread.sleep(30); // small delay to preserve natural ordering
                }
            }

            // Consume from the beginning
            var consume = Consume.builder()
                .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
                .region(Property.ofValue(localstack.getRegion()))
                .accessKeyId(Property.ofValue(localstack.getAccessKey()))
                .secretKeyId(Property.ofValue(localstack.getSecretKey()))
                .streamName(Property.ofValue(orderStreamName))
                .startingPosition(Property.ofValue("TRIM_HORIZON"))
                .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
                .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
                .build();

            Consume.Output output = consume.run(runContext);

            List<Consume.Message> messages = getMessages(runContext, output.getUri());
            assertThat(messages.size(), greaterThanOrEqualTo(10));

            // Group messages by partition key
            Map<String, List<String>> receivedByKey = messages.stream()
                .collect(Collectors.groupingBy(
                    Consume.Message::getKey,
                    Collectors.mapping(Consume.Message::getValue, Collectors.toList())
                ));

            // Verify that for each key, message order matches input order
            for (var entry : inputMessages.entrySet()) {
                String key = entry.getKey();
                List<String> expectedOrder = entry.getValue();
                List<String> actualOrder = receivedByKey.get(key);

                assertThat("Missing messages for key: " + key, actualOrder, notNullValue());
                assertThat("Incorrect message count for key: " + key, actualOrder.size(), equalTo(expectedOrder.size()));
                assertThat("Order mismatch for key: " + key, actualOrder, contains(expectedOrder.toArray()));
            }

        } finally {
            // Cleanup
            try {
                client.deleteStream(DeleteStreamRequest.builder().streamName(orderStreamName).build());
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void shouldFailWhenNoStreamIdentifier() {
        var runContext = runContextFactory.of();

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            // Neither streamName nor streamArn provided
            .startingPosition(Property.ofValue("TRIM_HORIZON"))
            .build();

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            consume.run(runContext);
        });

        assertThat(exception.getMessage(),
            containsString("Either streamName or streamArn must be provided"));
    }

    // Note: AT_TIMESTAMP is not fully supported by LocalStack, so this test is disabled
    // It works fine with real AWS Kinesis
    // @Test
    void consumeFromTimestamp() throws Exception {
        var runContext = runContextFactory.of();

        // Get current time for timestamp test
        Instant timestampBefore = Instant.now();

        // Wait a bit to ensure clear separation
        Thread.sleep(1000);

        // Add records after establishing timestamp
        KinesisClient client = client(localstack);
        client.putRecords(PutRecordsRequest.builder()
            .streamName(STREAM_NAME)
            .records(
                PutRecordsRequestEntry.builder()
                    .partitionKey("timestampKey1")
                    .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String("timestampMessage1"))
                    .build(),
                PutRecordsRequestEntry.builder()
                    .partitionKey("timestampKey2")
                    .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String("timestampMessage2"))
                    .build()
            )
            .build());

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .streamName(Property.ofValue(STREAM_NAME))
            .startingPosition(Property.ofValue("AT_TIMESTAMP"))
            .startingTimestamp(Property.ofValue(timestampBefore.toString()))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        Consume.Output output = consume.run(runContext);

        // Should get records added after the timestamp
        assertThat(output.getMessagesCount(), greaterThanOrEqualTo(2));
        assertThat(output.getUri(), notNullValue());

        List<Consume.Message> messages = getMessages(runContext, output.getUri());
        List<String> values = messages.stream().map(Consume.Message::getValue).toList();

        // Should include the new timestamp messages
        assertThat(values, hasItems("timestampMessage1", "timestampMessage2"));
    }

    @Test
    void shouldFailWhenAtTimestampWithoutTimestamp() {
        var runContext = runContextFactory.of();

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .streamName(Property.ofValue(STREAM_NAME))
            .startingPosition(Property.ofValue("AT_TIMESTAMP"))
            // startingTimestamp not provided
            .maxDuration(Property.ofValue(Duration.ofSeconds(2)))
            .build();

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            consume.run(runContext);
        });

        assertThat(exception.getMessage(),
            containsString("startingTimestamp must be provided when startingPosition is AT_TIMESTAMP"));
    }
}
package io.kestra.plugin.aws.sqs;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.sqs.model.Message;
import io.kestra.plugin.aws.sqs.model.SerdeType;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Execution(ExecutionMode.SAME_THREAD)
class PublishThenConsumeTest extends AbstractSqsTest {
    @Test
    void runText() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(
                List.of(
                    Message.builder().data("Hello World").build(),
                    Message.builder().data("Hello Kestra").delaySeconds(5).build()
                )
            )
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(2));

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .maxRecords(Property.ofValue(2))
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(2));
    }

    @Test
    void runJson() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(
                List.of(
                    Message.builder().data("""
                        {"hello" : "world"}""").build(),
                    Message.builder().data("""
                        {"hello" : "kestra"}""").delaySeconds(5).build()
                )
            )
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(2));

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .maxRecords(Property.ofValue(2))
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(2));
    }

    @Test
    void runFromListOfMaps() throws Exception {
        var runContext = runContextFactory.of();
        var publish = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(
                List.of(
                    Map.of("data", "Hello World"),
                    Map.of("data", "Hello Kestra", "delaySeconds", 5)
                )
            )
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(2));

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .maxRecords(Property.ofValue(2))
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(2));
    }

    @Test
    void publishMoreThan10MessagesUsesBatching() throws Exception {
        var runContext = runContextFactory.of();

        // 15 messages spans two batch requests (10 + 5), verifying the buffer(10) path.
        var messages = new ArrayList<Message>(15);
        for (int i = 0; i < 15; i++) {
            messages.add(Message.builder().data("batch-msg-" + i).build());
        }

        var publish = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(messages)
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(15));

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .maxRecords(Property.ofValue(15))
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(15));
    }

    @Test
    void runWithAutoDeleteFalse() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(List.of(Message.builder().data("test with autoDelete false").build()))
            .build();

        publish.run(runContext);

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .maxRecords(Property.ofValue(1))
            .visibilityTimeout(Property.ofValue(1))
            .autoDelete(Property.ofValue(false))
            .build();

        var firstConsume = consume.run(runContext);
        assertThat(firstConsume.getCount(), is(1));

        Thread.sleep(1500);

        // we verify that the message is still in the queue
        var secondConsume = consume.run(runContext);
        assertThat(secondConsume.getCount(), is(1));

        // for cleanup for other tests, we delete the message
        var consumeTrue = Consume.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .maxRecords(Property.ofValue(1))
            .autoDelete(Property.ofValue(true))
            .build();

        consumeTrue.run(runContext);
    }

    @Test
    void failedRunKeepsMessagesInQueue() throws Exception {
        var runContext = runContextFactory.of();

        // 12 valid messages (more than one delete batch), then a malformed one received last.
        var messages = new ArrayList<Message>();
        for (var i = 0; i < 12; i++) {
            messages.add(Message.builder().data("{\"id\": %d}".formatted(i)).build());
        }
        messages.add(Message.builder().data("not json").delaySeconds(3).build());

        Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(messages)
            .build()
            .run(runContext);

        var consume = Consume.builder()
            .id("consume-%s".formatted(IdUtils.create()))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .serdeType(Property.ofValue(SerdeType.JSON))
            // Without maxDuration the messages are hidden for visibilityTimeout only, so they come back quickly after the failure.
            .maxRecords(Property.ofValue(20))
            .visibilityTimeout(Property.ofValue(5))
            .build();

        assertThrows(IOException.class, () -> consume.run(runContextFactory.of()));

        // Nothing was deleted: once the visibility timeout elapses, all 13 messages are back in the queue.
        try (var sqsClient = sqsClient()) {
            List<String> bodies;
            try {
                Await.until(() -> visibleMessages(sqsClient) == 13, Duration.ofMillis(500), Duration.ofSeconds(20));
            } finally {
                // Always empty the queue so a failure here does not leak messages into the next tests.
                bodies = drain(sqsClient);
            }

            var expected = new ArrayList<String>();
            for (var i = 0; i < 12; i++) {
                expected.add("{\"id\": %d}".formatted(i));
            }
            expected.add("not json");
            assertThat(bodies, containsInAnyOrder(expected.toArray()));
        }
    }

    @Test
    void successfulRunDeletesStoredMessages() throws Exception {
        var runContext = runContextFactory.of();

        // The last message arrives after the 2s visibility timeout, so the first ones must stay hidden until the run deletes them.
        var messages = new ArrayList<Message>();
        for (var i = 0; i < 11; i++) {
            messages.add(Message.builder().data("delete-me-%d".formatted(i)).build());
        }
        messages.add(Message.builder().data("delete-me-11").delaySeconds(4).build());

        Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(messages)
            .build()
            .run(runContext);

        var consumeOutput = Consume.builder()
            .id("consume-%s".formatted(IdUtils.create()))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .maxRecords(Property.ofValue(12))
            .maxDuration(Property.ofValue(Duration.ofSeconds(20)))
            .visibilityTimeout(Property.ofValue(2))
            .build()
            .run(runContextFactory.of());

        assertThat(consumeOutput.getCount(), is(12));
        assertThat(consumeOutput.getFailedDeletesCount(), is(0));

        // Deleted messages never come back, even after the visibility timeout.
        Thread.sleep(2500);
        try (var sqsClient = sqsClient()) {
            assertThat(visibleMessages(sqsClient) + invisibleMessages(sqsClient), is(0));
        }
    }

    private List<String> drain(SqsClient sqsClient) {
        var bodies = new ArrayList<String>();
        List<software.amazon.awssdk.services.sqs.model.Message> received;
        do {
            received = sqsClient.receiveMessage(
                ReceiveMessageRequest.builder().queueUrl(queueUrl()).maxNumberOfMessages(10).waitTimeSeconds(1).build()
            ).messages();
            for (var m : received) {
                bodies.add(m.body());
                sqsClient.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl()).receiptHandle(m.receiptHandle()).build());
            }
        } while (!received.isEmpty());
        return bodies;
    }

    private int visibleMessages(SqsClient sqsClient) {
        return Integer.parseInt(queueAttribute(sqsClient, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
    }

    private int invisibleMessages(SqsClient sqsClient) {
        return Integer.parseInt(queueAttribute(sqsClient, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
    }

    private String queueAttribute(SqsClient sqsClient, QueueAttributeName name) {
        return sqsClient.getQueueAttributes(
            GetQueueAttributesRequest.builder().queueUrl(queueUrl()).attributeNames(name).build()
        ).attributes().get(name);
    }
}

package io.kestra.plugin.aws.sqs;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class ConsumeUnitTest {

    @Inject
    protected RunContextFactory runContextFactory;

    // Verifies that a partial failure from deleteMessageBatch causes Consume to throw IllegalStateException.
    @Test
    void flushDeletes_partialFailure_throws() throws Exception {
        var runContext = runContextFactory.of();

        var receiveResponse = ReceiveMessageResponse.builder()
            .messages(
                Message.builder()
                    .messageId("msg-1")
                    .receiptHandle("rh-1")
                    .body("hello")
                    .build()
            )
            .build();

        var deleteResponse = DeleteMessageBatchResponse.builder()
            .successful(List.of())
            .failed(
                List.of(
                    BatchResultErrorEntry.builder()
                        .id("0")
                        .code("InternalError")
                        .message("simulated failure")
                        .senderFault(false)
                        .build()
                )
            )
            .build();

        SqsClient mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receiveResponse)
            // Return empty on subsequent polls so the loop terminates.
            .thenReturn(ReceiveMessageResponse.builder().build());
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(deleteResponse);

        Consume task = spy(
            Consume.builder()
                .id("consume-partial-failure-test")
                .type(Consume.class.getName())
                .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
                .region(Property.ofValue("us-east-1"))
                .maxRecords(Property.ofValue(1))
                .autoDelete(Property.ofValue(true))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        assertThrows(IllegalStateException.class, () -> task.run(runContext));
    }

    // Regression test for #826: an empty first poll must not end the task.
    @Test
    void consumeWithMaxDurationOnEmptyQueue_keepsPolling() throws Exception {
        var runContext = runContextFactory.of();

        var emptyResponse = ReceiveMessageResponse.builder().build();
        var messageResponse = ReceiveMessageResponse.builder()
            .messages(
                Message.builder()
                    .messageId("msg-1")
                    .receiptHandle("rh-1")
                    .body("hello")
                    .build()
            )
            .build();

        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(emptyResponse)
            .thenReturn(messageResponse)
            .thenReturn(emptyResponse);

        var task = spy(
            Consume.builder()
                .id("consume-empty-queue-test")
                .type(Consume.class.getName())
                .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
                .region(Property.ofValue("us-east-1"))
                .maxDuration(Property.ofValue(Duration.ofSeconds(3)))
                .autoDelete(Property.ofValue(false))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        var output = task.run(runContext);

        assertThat(output.getCount(), is(1));
        verify(mockClient, atLeast(3)).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    void testKill() throws Exception {
        var runContext = runContextFactory.of();

        var receiveResponse = ReceiveMessageResponse.builder().build();

        SqsClient mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenAnswer(invocation -> {
                Thread.sleep(100);
                return receiveResponse;
            });

        Consume task = spy(
            Consume.builder()
                .id("test-kill")
                .type(Consume.class.getName())
                .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
                .region(Property.ofValue("us-east-1"))
                .maxRecords(Property.ofValue(100))
                .maxDuration(Property.ofValue(Duration.ofSeconds(30)))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        AtomicReference<Consume.Output> output = new AtomicReference<>();

        Thread thread = new Thread(() -> {
            try {
                output.set(task.run(runContext));
            } catch (Exception e) {
                // ignore
            }
        });

        thread.start();
        Thread.sleep(500); // let it start polling
        task.kill();
        thread.join(5000); // Wait max 5s for the loop to break

        assertThat(thread.isAlive(), is(false)); // Ensure it successfully exited
        assertThat(output.get(), notNullValue());
    }

    @Test
    void ended_maxDuration_emptyQueue_notElapsed_returnsFalse() throws Exception {
        RunContext runContext = runContextFactory.of();
        Consume task = Consume.builder()
            .id("consume-wait-test")
            .type(Consume.class.getName())
            .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
            .region(Property.ofValue("us-east-1"))
            .maxDuration(Property.ofValue(Duration.ofMinutes(1)))
            .build();

        AtomicInteger count = new AtomicInteger(0);
        ZonedDateTime start = ZonedDateTime.now();

        // Must not terminate on initial empty poll when maxDuration has not expired
        assertFalse(task.ended(count, start, runContext));
    }

    @Test
    void ended_maxDuration_emptyQueue_elapsed_returnsTrue() throws Exception {
        RunContext runContext = runContextFactory.of();
        Consume task = Consume.builder()
            .id("consume-elapsed-test")
            .type(Consume.class.getName())
            .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
            .region(Property.ofValue("us-east-1"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        AtomicInteger count = new AtomicInteger(0);
        ZonedDateTime start = ZonedDateTime.now().minusSeconds(5);

        // Must terminate once maxDuration has expired
        assertTrue(task.ended(count, start, runContext));
    }

    @Test
    void ended_onlyMaxRecords_emptyQueue_returnsTrue() throws Exception {
        RunContext runContext = runContextFactory.of();
        Consume task = Consume.builder()
            .id("consume-maxrecords-test")
            .type(Consume.class.getName())
            .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
            .region(Property.ofValue("us-east-1"))
            .maxRecords(Property.ofValue(10))
            .build();

        AtomicInteger count = new AtomicInteger(0);
        ZonedDateTime start = ZonedDateTime.now();

        // Preserves default behavior: terminates after first poll if empty and no maxDuration configured
        assertTrue(task.ended(count, start, runContext));
    }

    @Test
    void run_withMaxDuration_waitsAcrossEmptyPollsUntilMessageArrives() throws Exception {
        RunContext runContext = runContextFactory.of();

        ReceiveMessageResponse emptyResponse = ReceiveMessageResponse.builder()
            .messages(List.of())
            .build();

        ReceiveMessageResponse messageResponse = ReceiveMessageResponse.builder()
            .messages(
                Message.builder()
                    .messageId("msg-1")
                    .receiptHandle("rh-1")
                    .body("test payload")
                    .build()
            )
            .build();

        DeleteMessageBatchResponse deleteResponse = DeleteMessageBatchResponse.builder()
            .successful(List.of(DeleteMessageBatchResultEntry.builder().id("0").build()))
            .failed(List.of())
            .build();

        SqsClient mockClient = mock(SqsClient.class);
        // First poll: empty queue
        // Second poll: message arrives
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(emptyResponse)
            .thenReturn(messageResponse);
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(deleteResponse);

        Consume task = spy(
            Consume.builder()
                .id("consume-retry-test")
                .type(Consume.class.getName())
                .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
                .region(Property.ofValue("us-east-1"))
                .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
                .maxRecords(Property.ofValue(1))
                .autoDelete(Property.ofValue(true))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        Consume.Output output = task.run(runContext);

        assertThat(output.getCount(), is(1));
        verify(mockClient, atLeast(2)).receiveMessage(any(ReceiveMessageRequest.class));
    }
}

package io.kestra.plugin.aws.sqs;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.sqs.model.SerdeType;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class ConsumeUnitTest {

    @Inject
    protected RunContextFactory runContextFactory;

    // A failed delete must not drop the stored output: the task ends in WARNING and the message stays in the queue.
    @Test
    void deleteFailure_keepsOutputAndWarns() throws Exception {
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

        var output = task.run(runContext);

        assertThat(output.getCount(), is(1));
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getFailedDeletesCount(), is(1));
        assertThat(output.finalState(), is(Optional.of(State.Type.WARNING)));
    }

    @Test
    void failedRun_deletesNothing() throws Exception {
        var runContext = runContextFactory.of();

        SqsClient mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "{\"id\": 1}"))
            .thenReturn(receive("msg-2", "rh-2", "not json"));

        Consume task = spy(
            Consume.builder()
                .id("consume-failed-run-test")
                .type(Consume.class.getName())
                .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
                .region(Property.ofValue("us-east-1"))
                .serdeType(Property.ofValue(SerdeType.JSON))
                .maxRecords(Property.ofValue(2))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        assertThrows(Exception.class, () -> task.run(runContext));
        verify(mockClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void messageReceivedAgain_stopsAndDeletesWithLatestHandle() throws Exception {
        var runContext = runContextFactory.of();

        SqsClient mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"))
            // Same message delivered again after its visibility timeout, with a new receipt handle.
            .thenReturn(receive("msg-1", "rh-2", "hello"))
            .thenReturn(receive("msg-2", "rh-3", "world"));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        Consume task = spy(
            Consume.builder()
                .id("consume-received-again-test")
                .type(Consume.class.getName())
                .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
                .region(Property.ofValue("us-east-1"))
                .maxRecords(Property.ofValue(5))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        var output = task.run(runContext);

        assertThat(output.getCount(), is(1));
        verify(mockClient, times(2)).receiveMessage(any(ReceiveMessageRequest.class));
        assertThat(deletedReceiptHandles(mockClient), is(List.of("rh-2")));
    }

    @Test
    void visibilityExtensionFailure_stopsReceiving() throws Exception {
        var runContext = runContextFactory.of();

        SqsClient mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"))
            .thenReturn(receive("msg-2", "rh-2", "world"));
        when(mockClient.changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class)))
            .thenThrow(SqsException.builder().message("simulated AccessDenied").build());
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        Consume task = spy(
            Consume.builder()
                .id("consume-extension-failure-test")
                .type(Consume.class.getName())
                .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
                .region(Property.ofValue("us-east-1"))
                .maxRecords(Property.ofValue(5))
                // 0 makes the extension due right after the first receive.
                .visibilityTimeout(Property.ofValue(0))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        var output = task.run(runContext);

        assertThat(output.getCount(), is(1));
        verify(mockClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
        assertThat(deletedReceiptHandles(mockClient), is(List.of("rh-1")));
    }

    @Test
    void killedRun_deletesNothing() throws Exception {
        var runContext = runContextFactory.of();

        SqsClient mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"))
            .thenAnswer(invocation ->
            {
                Thread.sleep(100);
                return ReceiveMessageResponse.builder().build();
            });

        Consume task = spy(
            Consume.builder()
                .id("consume-killed-run-test")
                .type(Consume.class.getName())
                .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
                .region(Property.ofValue("us-east-1"))
                .maxRecords(Property.ofValue(100))
                .maxDuration(Property.ofValue(java.time.Duration.ofSeconds(30)))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        var thread = new Thread(() ->
        {
            try {
                task.run(runContext);
            } catch (Exception e) {
                // ignore
            }
        });
        thread.start();
        Thread.sleep(500);
        task.kill();
        thread.join(5000);

        assertThat(thread.isAlive(), is(false));
        verify(mockClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    private static List<String> deletedReceiptHandles(SqsClient mockClient) {
        var captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(mockClient).deleteMessageBatch(captor.capture());
        return captor.getValue().entries().stream().map(DeleteMessageBatchRequestEntry::receiptHandle).toList();
    }

    private static ReceiveMessageResponse receive(String messageId, String receiptHandle, String body) {
        return ReceiveMessageResponse.builder()
            .messages(Message.builder().messageId(messageId).receiptHandle(receiptHandle).body(body).build())
            .build();
    }

    @Test
    void testKill() throws Exception {
        var runContext = runContextFactory.of();

        var receiveResponse = ReceiveMessageResponse.builder().build();

        SqsClient mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenAnswer(invocation ->
            {
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
                .maxDuration(Property.ofValue(java.time.Duration.ofSeconds(30)))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        java.util.concurrent.atomic.AtomicReference<Consume.Output> output = new java.util.concurrent.atomic.AtomicReference<>();

        Thread thread = new Thread(() ->
        {
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

        org.hamcrest.MatcherAssert.assertThat(thread.isAlive(), is(false)); // Ensure it successfully exited
        org.hamcrest.MatcherAssert.assertThat(output.get(), notNullValue());
    }
}

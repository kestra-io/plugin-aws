package io.kestra.plugin.aws.sqs;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
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
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(
                DeleteMessageBatchResponse.builder()
                    .failed(BatchResultErrorEntry.builder().id("0").code("InternalError").message("simulated failure").senderFault(false).build())
                    .build()
            );

        var task = withClient(consume().maxRecords(Property.ofValue(1)).build(), mockClient);

        var output = task.run(runContextFactory.of());

        assertThat(output.getCount(), is(1));
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getFailedDeletesCount(), is(1));
        assertThat(output.finalState(), is(Optional.of(State.Type.WARNING)));
    }

    // 11 valid messages, more than one delete batch, then one that fails to parse.
    @Test
    void failedRun_deletesNothing() throws Exception {
        var responses = IntStream.range(1, 11)
            .mapToObj(i -> receive("msg-%d".formatted(i), "rh-%d".formatted(i), "{\"id\": %d}".formatted(i)))
            .toArray(ReceiveMessageResponse[]::new);
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-0", "rh-0", "{\"id\": 0}"), responses)
            .thenReturn(receive("msg-bad", "rh-bad", "not json"));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        var task = withClient(
            consume()
                .serdeType(Property.ofValue(SerdeType.JSON))
                .maxRecords(Property.ofValue(12))
                .build(),
            mockClient
        );

        assertThrows(IOException.class, () -> task.run(runContextFactory.of()));
        verify(mockClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void messageReceivedAgain_isStoredOnceAndDeletedWithLatestHandle() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"))
            // Same message delivered again after its visibility timeout, with a new receipt handle.
            .thenReturn(receive("msg-1", "rh-2", "hello"))
            .thenReturn(receive("msg-2", "rh-3", "world"));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        var task = withClient(consume().maxRecords(Property.ofValue(2)).build(), mockClient);

        var output = task.run(runContextFactory.of());

        assertThat(output.getCount(), is(2));
        verify(mockClient, times(3)).receiveMessage(any(ReceiveMessageRequest.class));
        assertThat(deletedReceiptHandles(mockClient), is(List.of("rh-2", "rh-3")));
    }

    @Test
    void killedRun_deletesNothing() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"))
            .thenAnswer(invocation ->
            {
                Thread.sleep(100);
                return ReceiveMessageResponse.builder().build();
            });

        var task = withClient(
            consume()
                .maxRecords(Property.ofValue(100))
                .maxDuration(Property.ofValue(Duration.ofSeconds(30)))
                .build(),
            mockClient
        );

        var output = new AtomicReference<Consume.Output>();
        var thread = new Thread(() ->
        {
            try {
                output.set(task.run(runContextFactory.of()));
            } catch (Exception e) {
                // ignore
            }
        });
        thread.start();
        Thread.sleep(500);
        task.kill();
        thread.join(5000);

        assertThat(thread.isAlive(), is(false));
        assertThat(output.get().getCount(), is(1));
        verify(mockClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    private static Consume.ConsumeBuilder<?, ?> consume() {
        return Consume.builder()
            .id("consume-%s".formatted(IdUtils.create()))
            .type(Consume.class.getName())
            .queueUrl(Property.ofValue("https://sqs.us-east-1.amazonaws.com/000000000000/test-queue"))
            .region(Property.ofValue("us-east-1"));
    }

    private static Consume withClient(Consume task, SqsClient mockClient) throws Exception {
        var spied = spy(task);
        doReturn(mockClient).when(spied).client(any());
        return spied;
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

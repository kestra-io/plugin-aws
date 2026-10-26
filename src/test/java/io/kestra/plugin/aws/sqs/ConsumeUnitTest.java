package io.kestra.plugin.aws.sqs;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class ConsumeUnitTest {

    @Inject
    protected RunContextFactory runContextFactory;

    // A partial delete failure must not drop the stored output: the task ends in WARNING and the message stays in the queue.
    @Test
    void partialDeleteFailure_keepsOutputAndWarns() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"), receive("msg-2", "rh-2", "world"));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(
                DeleteMessageBatchResponse.builder()
                    .failed(BatchResultErrorEntry.builder().id("0").code("InternalError").message("simulated failure").senderFault(false).build())
                    .build()
            );

        var task = withClient(consume().maxRecords(Property.ofValue(2)).build(), mockClient);

        var output = task.run(runContextFactory.of());

        assertThat(output.getCount(), is(2));
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getFailedDeletesCount(), is(1));
        assertThat(output.finalState(), is(Optional.of(State.Type.WARNING)));
    }

    @Test
    void failedDeleteCall_countsItsBatch() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-0", "rh-0", "m"), receives(1, 11));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build())
            .thenThrow(SqsException.builder().message("simulated outage").build());

        var task = withClient(consume().maxRecords(Property.ofValue(11)).build(), mockClient);

        var output = task.run(runContextFactory.of());

        assertThat(output.getCount(), is(11));
        assertThat(output.getFailedDeletesCount(), is(1));
        assertThat(output.finalState(), is(Optional.of(State.Type.WARNING)));
    }

    // With no message deleted, for example without sqs:DeleteMessage, a trigger must not create the same execution on every poll.
    @Test
    void allDeletesFailing_failsTheRun() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenThrow(SqsException.builder().message("simulated AccessDenied").build());

        var task = withClient(consume().maxRecords(Property.ofValue(1)).build(), mockClient);

        var exception = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("sqs:DeleteMessage"));
    }

    // 11 valid messages, more than one delete batch, then one that fails to parse.
    @Test
    void failedRun_deletesNothing() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-0", "rh-0", "{\"id\": 0}"), receives(1, 11))
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
    void messageReceivedAgain_stopsAndDeletesWithLatestHandle() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"))
            // Same message delivered again after its hidden time ran out, with a new receipt handle.
            .thenReturn(receive("msg-1", "rh-2", "hello"))
            .thenReturn(receive("msg-2", "rh-3", "world"));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        var task = withClient(consume().maxRecords(Property.ofValue(5)).build(), mockClient);

        var output = task.run(runContextFactory.of());

        assertThat(output.getCount(), is(1));
        verify(mockClient, times(2)).receiveMessage(any(ReceiveMessageRequest.class));
        assertThat(deletedReceiptHandles(mockClient), is(List.of("rh-2")));
    }

    @Test
    void receive_hidesMessagesUntilTheRunEnds() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        var task = withClient(
            consume()
                .maxRecords(Property.ofValue(1))
                .maxDuration(Property.ofValue(Duration.ofSeconds(60)))
                .build(),
            mockClient
        );

        task.run(runContextFactory.of());

        // The rest of the 60s run plus the default 30s visibility timeout.
        assertThat(receivedVisibilityTimeout(mockClient), allOf(greaterThanOrEqualTo(85), lessThanOrEqualTo(90)));
    }

    @Test
    void receive_capsHiddenTimeAtTheSqsMaximum() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        var task = withClient(
            consume()
                .maxRecords(Property.ofValue(1))
                .maxDuration(Property.ofValue(Duration.ofHours(24)))
                .build(),
            mockClient
        );

        task.run(runContextFactory.of());

        assertThat(receivedVisibilityTimeout(mockClient), is(Consume.MAX_VISIBILITY_TIMEOUT));
    }

    @Test
    void receive_keepsVisibilityTimeoutWithoutAutoDeleteOrMaxDuration() throws Exception {
        var withoutAutoDelete = mock(SqsClient.class);
        when(withoutAutoDelete.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"));
        withClient(
            consume()
                .autoDelete(Property.ofValue(false))
                .maxRecords(Property.ofValue(1))
                .maxDuration(Property.ofValue(Duration.ofSeconds(60)))
                .build(),
            withoutAutoDelete
        ).run(runContextFactory.of());

        var withoutMaxDuration = mock(SqsClient.class);
        when(withoutMaxDuration.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(receive("msg-1", "rh-1", "hello"));
        when(withoutMaxDuration.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());
        withClient(consume().maxRecords(Property.ofValue(1)).build(), withoutMaxDuration).run(runContextFactory.of());

        assertThat(receivedVisibilityTimeout(withoutAutoDelete), is(30));
        assertThat(receivedVisibilityTimeout(withoutMaxDuration), is(30));
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

    @Test
    void maxDuration_keepsPollingThroughEmptyPolls() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().build(), receive("msg-1", "rh-1", "hello"));
        when(mockClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());

        var task = withClient(
            consume()
                .maxRecords(Property.ofValue(1))
                .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
                .build(),
            mockClient
        );

        var output = task.run(runContextFactory.of());

        assertThat(output.getCount(), is(1));
        verify(mockClient, times(2)).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    void maxDuration_emptyQueue_stopsOnceElapsed() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().build());

        var task = withClient(consume().maxDuration(Property.ofValue(Duration.ofSeconds(2))).build(), mockClient);

        var started = Instant.now();
        var output = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> task.run(runContextFactory.of()));

        assertThat(output.getCount(), is(0));
        assertThat(Duration.between(started, Instant.now()), greaterThanOrEqualTo(Duration.ofSeconds(2)));
        verify(mockClient, atLeast(2)).receiveMessage(any(ReceiveMessageRequest.class));
        verify(mockClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void maxRecordsOnly_emptyQueue_stopsAfterFirstPoll() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().build());

        var task = withClient(consume().maxRecords(Property.ofValue(10)).build(), mockClient);

        var output = task.run(runContextFactory.of());

        assertThat(output.getCount(), is(0));
        verify(mockClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
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

    private static int receivedVisibilityTimeout(SqsClient mockClient) {
        var captor = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        verify(mockClient, atLeastOnce()).receiveMessage(captor.capture());
        return captor.getAllValues().getFirst().visibilityTimeout();
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

    // One JSON message per response, ids from start inclusive to end exclusive.
    private static ReceiveMessageResponse[] receives(int start, int end) {
        return IntStream.range(start, end)
            .mapToObj(i -> receive("msg-%d".formatted(i), "rh-%d".formatted(i), "{\"id\": %d}".formatted(i)))
            .toArray(ReceiveMessageResponse[]::new);
    }
}

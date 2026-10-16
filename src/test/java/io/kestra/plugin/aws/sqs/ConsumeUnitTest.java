package io.kestra.plugin.aws.sqs;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
                .maxDuration(Property.ofValue(java.time.Duration.ofSeconds(30)))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        java.util.concurrent.atomic.AtomicReference<Consume.Output> output = new java.util.concurrent.atomic.AtomicReference<>();
        
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

        org.hamcrest.MatcherAssert.assertThat(thread.isAlive(), is(false)); // Ensure it successfully exited
        org.hamcrest.MatcherAssert.assertThat(output.get(), notNullValue());
    }
}

package io.kestra.plugin.aws.sqs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.sqs.model.Message;
import io.kestra.plugin.aws.sqs.model.SerdeType;

import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests RealtimeTrigger.publisher() JSON deserialization in isolation,
 * without starting the full Kestra runner/scheduler to avoid inter-test
 * message consumption by other active triggers.
 */
@KestraTest
class RealtimeTriggerPublisherTest extends AbstractSqsTest {

    @BeforeEach
    void purgeQueue() {
        try (var sqsClient = SqsClient.builder()
            .endpointOverride(java.net.URI.create(endpointUrl()))
            .region(Region.of(REGION))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
            .build()) {
            sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl()).build());
        }
    }

    @Test
    void publisherWithJsonSerdeTypeDeserializesBody() throws Exception {
        var runContext = runContextFactory.of();

        Publish publish = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(List.of(Message.builder().data("{\"order_id\":\"42\",\"customer\":\"Alice\"}").build()))
            .build();
        publish.run(runContext);

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("test-rt-json")
            .type(RealtimeTrigger.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .build();

        Consume task = Consume.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .build();

        CountDownLatch received = new CountDownLatch(1);
        var messages = new CopyOnWriteArrayList<Message>();

        trigger.publisher(task, runContext)
            .subscribe(msg -> {
                messages.add(msg);
                received.countDown();
                trigger.stop();
            });

        boolean got = received.await(1, TimeUnit.MINUTES);
        assertThat("timed out waiting for JSON message from publisher", got, is(true));
        assertThat(messages.size(), is(1));
        var data = messages.getFirst().getData();
        assertThat(data, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        var map = (Map<String, Object>) data;
        assertThat(map.get("order_id"), is("42"));
        assertThat(map.get("customer"), is("Alice"));
    }

    /**
     * A transient receive failure must not terminate the publisher. The loop must
     * recover and continue polling after the backoff.
     *
     * We inject a mock SqsAsyncClient that fails on the first receiveMessage call
     * (simulating a transient error) and succeeds on subsequent calls by delegating
     * to a real long-lived client.
     *
     * The publisher runs on a separate thread (subscribeOn) so the main test thread
     * can await the result independently.
     */
    @Test
    @Timeout(30)
    void transientReceiveErrorDoesNotTerminatePublisher() throws Exception {
        var runContext = runContextFactory.of();

        // Publish one real message that we expect to receive after the transient failure.
        Publish publish = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(List.of(Message.builder().data("after-transient").build()))
            .build();
        publish.run(runContext);

        var realConsumeTask = Consume.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .build();

        // Keep one real client open for the duration so delegated calls have a live connection pool.
        var realClient = realConsumeTask.asyncClient(runContext);
        try {
            var callCount = new AtomicInteger(0);
            var mockClient = mock(SqsAsyncClient.class);

            // First call: fail with a transient exception.
            // Subsequent calls: delegate to the shared real client.
            when(mockClient.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(inv -> {
                int call = callCount.incrementAndGet();
                if (call == 1) {
                    var failed = new CompletableFuture<ReceiveMessageResponse>();
                    failed.completeExceptionally(new RuntimeException("simulated transient network error"));
                    return failed;
                }
                return realClient.receiveMessage((ReceiveMessageRequest) inv.getArgument(0));
            });
            when(mockClient.deleteMessage(any(DeleteMessageRequest.class))).thenAnswer(inv ->
                realClient.deleteMessage((DeleteMessageRequest) inv.getArgument(0))
            );

            Consume consumeWithMock = new Consume() {
                @Override
                public SqsAsyncClient asyncClient(RunContext rc, int retryMaxAttempts)
                    throws io.kestra.core.exceptions.IllegalVariableEvaluationException {
                    return mockClient;
                }
            };

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-rt-transient")
                .type(RealtimeTrigger.class.getName())
                .endpointOverride(Property.ofValue(endpointUrl()))
                .queueUrl(Property.ofValue(queueUrl()))
                .region(Property.ofValue(REGION))
                .accessKeyId(Property.ofValue(ACCESS_KEY))
                .secretKeyId(Property.ofValue(SECRET_KEY))
                .build();

            CountDownLatch received = new CountDownLatch(1);
            var messages = new CopyOnWriteArrayList<Message>();

            // subscribeOn so the publisher loop runs on its own thread, not the test thread.
            trigger.publisher(consumeWithMock, runContext)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(msg -> {
                    messages.add(msg);
                    received.countDown();
                    trigger.stop();
                });

            boolean got = received.await(20, TimeUnit.SECONDS);
            assertThat("publisher did not recover after transient error", got, is(true));
            assertThat(messages.getFirst().getData(), is("after-transient"));
        } finally {
            realClient.close();
        }
    }

    /**
     * stop() must return promptly even when the trigger is mid-backoff.
     * The backoff slice is 200 ms; we give stop() 500 ms to complete.
     */
    @Test
    @Timeout(10)
    void stopReturnsDuringBackoff() throws Exception {
        var runContext = runContextFactory.of();

        // A mock client that always fails so the publisher enters backoff immediately.
        var mockClient = mock(SqsAsyncClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(inv -> {
            var failed = new CompletableFuture<ReceiveMessageResponse>();
            failed.completeExceptionally(new RuntimeException("always failing"));
            return failed;
        });

        Consume consumeWithMock = new Consume() {
            @Override
            public SqsAsyncClient asyncClient(RunContext rc, int retryMaxAttempts)
                throws io.kestra.core.exceptions.IllegalVariableEvaluationException {
                return mockClient;
            }
        };

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("test-rt-stop-backoff")
            .type(RealtimeTrigger.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .build();

        var errors = new CopyOnWriteArrayList<Throwable>();
        // Run the publisher on a background thread so the test thread is free to call stop().
        trigger.publisher(consumeWithMock, runContext)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(msg -> {}, errors::add);

        // Wait until the publisher has entered the backoff sleep (at least one failure recorded).
        Thread.sleep(300);

        long stopStart = System.currentTimeMillis();
        trigger.stop();
        long elapsed = System.currentTimeMillis() - stopStart;

        // stop() must return well within one full backoff (1 s); 500 ms is a generous bound.
        assertThat("stop() blocked longer than expected during backoff", elapsed < 500, is(true));
    }
}

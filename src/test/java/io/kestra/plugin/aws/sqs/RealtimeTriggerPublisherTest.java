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
import software.amazon.awssdk.services.sqs.model.SqsException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
     * A transient receive failure must not terminate the publisher. It recovers and keeps delivering.
     */
    @Test
    @Timeout(30)
    void transientReceiveErrorDoesNotTerminatePublisher() throws Exception {
        var runContext = runContextFactory.of();

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

            trigger.publisher(consumeWithClient(mockClient), runContext)
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
     */
    @Test
    @Timeout(10)
    void stopReturnsDuringBackoff() throws Exception {
        var runContext = runContextFactory.of();

        var mockClient = mock(SqsAsyncClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(inv -> {
            var failed = new CompletableFuture<ReceiveMessageResponse>();
            failed.completeExceptionally(new RuntimeException("always failing"));
            return failed;
        });

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("test-rt-stop-backoff")
            .type(RealtimeTrigger.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .build();

        trigger.publisher(consumeWithClient(mockClient), runContext)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(msg -> {}, err -> {});

        Thread.sleep(300);

        long stopStart = System.currentTimeMillis();
        trigger.stop();
        long elapsed = System.currentTimeMillis() - stopStart;

        assertThat("stop() blocked longer than expected during backoff", elapsed < 500, is(true));
    }

    /**
     * A 403 (AccessDenied) SqsException must terminate the publisher with onError, not retry.
     */
    @Test
    @Timeout(10)
    void fatalSqsErrorTerminatesPublisher() throws Exception {
        var runContext = runContextFactory.of();

        var fatalException = SqsException.builder()
            .statusCode(403)
            .message("Access to the resource https://sqs.us-east-1.amazonaws.com/000/q is denied.")
            .build();

        var mockClient = mock(SqsAsyncClient.class);
        when(mockClient.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(inv -> {
            var failed = new CompletableFuture<ReceiveMessageResponse>();
            failed.completeExceptionally(fatalException);
            return failed;
        });

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("test-rt-fatal-403")
            .type(RealtimeTrigger.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .build();

        var errors = new CopyOnWriteArrayList<Throwable>();
        var terminated = new CountDownLatch(1);

        trigger.publisher(consumeWithClient(mockClient), runContext)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                msg -> {},
                err -> { errors.add(err); terminated.countDown(); },
                terminated::countDown
            );

        assertThat("publisher did not terminate after fatal 403", terminated.await(5, TimeUnit.SECONDS), is(true));
        assertThat("publisher must signal onError for fatal exceptions", errors.size(), is(1));
        assertThat(errors.getFirst(), instanceOf(SqsException.class));
    }

    /**
     * A malformed (non-JSON) message must be emitted with the raw body and then deleted so it
     * never reappears (poison-message loop prevention).
     */
    @Test
    @Timeout(60)
    void poisonMessageIsEmittedRawAndDeleted() throws Exception {
        var runContext = runContextFactory.of();

        // Publish a non-JSON body directly via the AWS SDK to bypass SerdeType serialization.
        try (var sqsClient = SqsClient.builder()
            .endpointOverride(java.net.URI.create(endpointUrl()))
            .region(Region.of(REGION))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
            .build()) {
            sqsClient.sendMessage(b -> b.queueUrl(queueUrl()).messageBody("not-json"));
        }

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("test-rt-poison")
            .type(RealtimeTrigger.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .autoDelete(Property.ofValue(true))
            .build();

        Consume task = Consume.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .autoDelete(Property.ofValue(true))
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
        assertThat("timed out waiting for poison message", got, is(true));
        assertThat("exactly one execution must be emitted for the poison message", messages.size(), is(1));
        assertThat("raw body must be a String", messages.getFirst().getData(), instanceOf(String.class));
        assertThat("raw body value must match the original payload", messages.getFirst().getData(), is("not-json"));

        // Confirm the message was deleted: queue must be empty after a short drain wait.
        try (var sqsClient = SqsClient.builder()
            .endpointOverride(java.net.URI.create(endpointUrl()))
            .region(Region.of(REGION))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
            .build()) {
            var remaining = sqsClient.receiveMessage(b -> b
                .queueUrl(queueUrl())
                .maxNumberOfMessages(10)
                .waitTimeSeconds(2));
            assertThat("queue must be empty after poison message was deleted", remaining.messages().isEmpty(), is(true));
        }
    }

    private static Consume consumeWithClient(SqsAsyncClient client) {
        return new Consume() {
            @Override
            public SqsAsyncClient asyncClient(RunContext rc, int retryMaxAttempts)
                throws io.kestra.core.exceptions.IllegalVariableEvaluationException {
                return client;
            }
        };
    }
}

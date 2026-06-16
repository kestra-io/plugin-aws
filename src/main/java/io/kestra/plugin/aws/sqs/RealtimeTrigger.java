package io.kestra.plugin.aws.sqs;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.kestra.plugin.aws.sqs.model.Message;
import io.kestra.plugin.aws.sqs.model.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on SQS messages (realtime)",
    description = "Long-polls SQS and emits an execution per message as they arrive. Auto-delete controls deletion; use batch Trigger for grouped processing."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Consume a message from an SQS queue in real-time.",
            code = """
                id: sqs
                namespace: company.team

                tasks:
                - id: log
                  type: io.kestra.plugin.core.log.Log
                  message: "{{ trigger.data }}"

                triggers:
                - id: realtime_trigger
                  type: io.kestra.plugin.aws.sqs.RealtimeTrigger
                  accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                  secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                  region: "{{ secret('AWS_DEFAULT_REGION') }}"
                  queueUrl: https://sqs.eu-central-1.amazonaws.com/000000000000/test-queue
                """
        ),
        @Example(
            full = true,
            title = "Use AWS SQS Realtime Trigger to push events into DynamoDB",
            code = """
                id: sqs_realtime_trigger
                namespace: company.team

                tasks:
                  - id: insert_into_dynamoDB
                    type: io.kestra.plugin.aws.dynamodb.PutItem
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: eu-central-1
                    tableName: orders
                    item:
                      order_id: "{{ trigger.data | jq('.order_id') | first }}"
                      customer_name: "{{ trigger.data | jq('.customer_name') | first }}"
                      customer_email: "{{ trigger.data | jq('.customer_email') | first }}"
                      product_id: "{{ trigger.data | jq('.product_id') | first }}"
                      price: "{{ trigger.data | jq('.price') | first }}"
                      quantity: "{{ trigger.data | jq('.quantity') | first }}"
                      total: "{{ trigger.data | jq('.total') | first }}"

                triggers:
                  - id: realtime_trigger
                    type: io.kestra.plugin.aws.sqs.RealtimeTrigger
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: eu-central-1
                    queueUrl: https://sqs.eu-central-1.amazonaws.com/000000000000/orders
                    serdeType: JSON
                """
        )

    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Message>, SqsConnectionInterface {

    private static final Duration POLL_ERROR_BACKOFF_BASE = Duration.ofSeconds(1);
    private static final Duration POLL_ERROR_BACKOFF_MAX = Duration.ofSeconds(30);
    private static final Duration POLL_SLEEP_SLICE = Duration.ofMillis(200);

    private Property<String> queueUrl;

    private Property<String> accessKeyId;

    private Property<String> secretKeyId;

    private Property<String> sessionToken;

    private Property<String> region;

    private Property<String> endpointOverride;

    @Builder.Default
    private Property<Integer> maxConcurrency = Property.ofValue(50);

    @Builder.Default
    private Property<Duration> connectionAcquisitionTimeout = Property.ofValue(Duration.ofSeconds(5));

    @Builder.Default
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    @PluginProperty(group = "main")
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    // Configuration for AWS STS AssumeRole
    protected Property<String> stsRoleArn;
    protected Property<String> stsRoleExternalId;
    protected Property<String> stsRoleSessionName;
    protected Property<String> stsEndpointOverride;
    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    // Default read timeout is 20s, so we cannot use a bigger wait time, or we would need to increase the read timeout.
    @Schema(title = "The duration for which the SQS client waits for a message.")
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Duration> waitTime = Property.ofValue(Duration.ofSeconds(20));

    @Schema(
        title = "The maximum number of messages returned from request made to SQS.",
        description = "Increasing this value can reduce the number of requests made to SQS. Amazon SQS never returns more messages than this value (fewer messages might be returned). Valid values: 1 to 10. Setting this value to 1 would increase your AWS cost and latency because it requires more API requests to SQS. **Note that Realtime Triggers always create one execution per message, regardless of the value of this property.**"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Integer> maxNumberOfMessage = Property.ofValue(5);

    @Schema(
        title = "The maximum number of attempts used by the SQS client's retry strategy."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> clientRetryMaxAttempts = Property.ofValue(3);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Schema(
        title = "Delete consumed messages automatically.",
        description = "When set to true (default), the message is automatically deleted from SQS after being consumed. Set to false if you want to handle deletion manually."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> autoDelete = Property.ofValue(true);

    @Schema(
        title = "Visibility timeout for consumed messages.",
        description = "When set, a received message stays hidden from other consumers for this amount of time (in seconds). The default value is 30 seconds."

    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> visibilityTimeout = Property.ofValue(30);

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        Consume task = Consume.builder()
            .queueUrl(queueUrl)
            .accessKeyId(accessKeyId)
            .secretKeyId(secretKeyId)
            .sessionToken(sessionToken)
            .region(region)
            .endpointOverride(endpointOverride)
            .serdeType(this.serdeType)
            .stsRoleArn(this.stsRoleArn)
            .stsRoleSessionName(this.stsRoleSessionName)
            .stsRoleExternalId(this.stsRoleExternalId)
            .stsRoleSessionDuration(this.stsRoleSessionDuration)
            .stsEndpointOverride(this.stsEndpointOverride)
            .autoDelete(this.autoDelete)
            .visibilityTimeout(this.visibilityTimeout)
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map(record -> TriggerService.generateRealtimeExecution(this, conditionContext, context, record));
    }

    public Flux<Message> publisher(final Consume task,
        final RunContext runContext) throws Exception {
        var renderedQueueUrl = runContext.render(getQueueUrl()).as(String.class).orElseThrow();

        return Flux.create(fluxSink -> {
            var logger = runContext.logger();
            var signalledError = false;
            try (var sqsClient = task.asyncClient(runContext, runContext.render(clientRetryMaxAttempts).as(Integer.class).orElseThrow())) {
                var rWaitTimeSeconds = (int) runContext.render(waitTime).as(Duration.class).orElseThrow().toSeconds();
                var rMaxNumberOfMessages = runContext.render(maxNumberOfMessage).as(Integer.class).orElseThrow();
                var rVisibilityTimeout = runContext.render(visibilityTimeout).as(Integer.class).orElse(30);
                var rAutoDelete = runContext.render(autoDelete).as(Boolean.class).orElse(true);
                var rSerdeType = runContext.render(serdeType).as(SerdeType.class).orElse(SerdeType.STRING);

                logger.info("Starting SQS consumption from queue '{}'.", renderedQueueUrl);

                var currentBackoff = POLL_ERROR_BACKOFF_BASE;

                while (isActive.get()) {
                    var receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(renderedQueueUrl)
                        .waitTimeSeconds(rWaitTimeSeconds)
                        .maxNumberOfMessages(rMaxNumberOfMessages)
                        .visibilityTimeout(rVisibilityTimeout)
                        .build();

                    try {
                        var response = sqsClient.receiveMessage(receiveRequest).get();

                        currentBackoff = POLL_ERROR_BACKOFF_BASE;

                        if (!response.messages().isEmpty()) {
                            logger.debug("Received {} message(s) from queue '{}'.", response.messages().size(), renderedQueueUrl);
                        }

                        emitAndDelete(fluxSink, sqsClient, renderedQueueUrl, response.messages(), rAutoDelete, rSerdeType, logger);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        isActive.set(false); // proactively stop polling
                    } catch (ExecutionException e) {
                        var cause = e.getCause() != null ? e.getCause() : e;
                        if (cause instanceof SqsException sqsEx && isFatal(sqsEx)) {
                            logger.error("Fatal SQS error on queue '{}': {}. Stopping trigger.", renderedQueueUrl, cause.getMessage());
                            signalledError = true;
                            isActive.set(false);
                            fluxSink.error(cause);
                        } else {
                            logger.warn("Transient error while polling queue '{}': {}. Retrying in {}.", renderedQueueUrl, cause.getMessage(), currentBackoff);
                            sleepInterruptibly(currentBackoff);
                            currentBackoff = min(currentBackoff.multipliedBy(2), POLL_ERROR_BACKOFF_MAX);
                        }
                    }
                }
            } catch (Throwable e) {
                signalledError = true;
                fluxSink.error(e);
            } finally {
                if (!signalledError) {
                    fluxSink.complete();
                }
                this.waitForTermination.countDown();
            }
        });
    }

    private void emitAndDelete(
        FluxSink<Message> sink,
        SqsAsyncClient client,
        String queueUrl,
        List<software.amazon.awssdk.services.sqs.model.Message> messages,
        boolean autoDelete,
        SerdeType serdeType,
        Logger logger
    ) {
        for (var message : messages) {
            Object body;
            try {
                body = serdeType.deserialize(message.body());
            } catch (IOException e) {
                logger.warn("Failed to deserialize SQS message body (skipping): {}", e.getMessage());
                continue;
            }

            sink.next(Message.builder().data(body).build());

            if (autoDelete) {
                try {
                    client.deleteMessage(
                        DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build()
                    ).get();
                } catch (InterruptedException ie) {
                    // Restore interrupt flag and shut down cleanly.
                    Thread.currentThread().interrupt();
                    isActive.set(false);
                    break;
                } catch (ExecutionException de) {
                    // Accept at-least-once redelivery instead of routing delete failures to backoff.
                    var cause = de.getCause() != null ? de.getCause() : de;
                    logger.warn("Failed to delete SQS message (will be redelivered): {}", cause.getMessage());
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }
        if (wait) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // Sliced so stop() (isActive=false) cuts the wait within one slice, not the full backoff.
    private void sleepInterruptibly(Duration delay) {
        var remaining = delay.toMillis();
        while (isActive.get() && remaining > 0) {
            var sleepMs = Math.min(remaining, POLL_SLEEP_SLICE.toMillis());
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                isActive.set(false);
                return;
            }
            remaining -= sleepMs;
        }
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static boolean isFatal(SqsException ex) {
        // client-side errors (4xx) other than throttling are config/permission problems that retrying will not fix
        return ex.statusCode() >= 400 && ex.statusCode() < 500 && !ex.isThrottlingException();
    }
}

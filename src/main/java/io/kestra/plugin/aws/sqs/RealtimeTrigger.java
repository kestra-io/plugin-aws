package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume a message in real-time from an SQS queue and create one execution per message.",
    description = "If you would like to consume multiple messages processed within a given time frame and process them in batch, you can use the [io.kestra.plugin.aws.sqs.Trigger](https://kestra.io/plugins/plugin-aws/triggers/io.kestra.plugin.aws.sqs.trigger) instead."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume a message from an SQS queue in real-time.",
            full = true,
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
                  accessKeyId: "access_key"
                  secretKeyId: "secret_key"
                  region: "eu-central-1"
                  queueUrl: https://sqs.eu-central-1.amazonaws.com/000000000000/test-queue"""
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Message>, SqsConnectionInterface {

    private Property<String> queueUrl;

    private Property<String> accessKeyId;

    private Property<String> secretKeyId;

    private Property<String> sessionToken;

    private Property<String> region;

    private Property<String> endpointOverride;

    @Builder.Default
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private Property<SerdeType> serdeType = Property.of(SerdeType.STRING);

    // Configuration for AWS STS AssumeRole
    protected Property<String> stsRoleArn;
    protected Property<String> stsRoleExternalId;
    protected Property<String> stsRoleSessionName;
    protected Property<String> stsEndpointOverride;
    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.of(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    // Default read timeout is 20s, so we cannot use a bigger wait time, or we would need to increase the read timeout.
    @Schema(title = "The duration for which the SQS client waits for a message.")
    @Builder.Default
    protected Property<Duration> waitTime = Property.of(Duration.ofSeconds(20));

    @Schema(
        title = "The maximum number of messages returned from request made to SQS.",
        description = "Increasing this value can reduce the number of requests made to SQS. Amazon SQS never returns more messages than this value (however, fewer messages might be returned). Valid values: 1 to 10."
    )
    @Builder.Default
    protected Property<Integer> maxNumberOfMessage = Property.of(5);

    @Schema(
        title = "The maximum number of attempts used by the SQS client's retry strategy."
    )
    @Builder.Default
    protected Property<Integer> clientRetryMaxAttempts = Property.of(3);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

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
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map(record -> TriggerService.generateRealtimeExecution(this, conditionContext, context, record));
    }

    public Flux<Message> publisher(final Consume task,
                                   final RunContext runContext) throws Exception {
        var renderedQueueUrl = runContext.render(getQueueUrl()).as(String.class).orElseThrow();

        return Flux.create(
            fluxSink -> {
                try (SqsAsyncClient sqsClient = task.asyncClient(runContext, runContext.render(clientRetryMaxAttempts).as(Integer.class).orElseThrow())) {
                    while (isActive.get()) {
                        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                            .queueUrl(renderedQueueUrl)
                            .waitTimeSeconds((int) runContext.render(waitTime).as(Duration.class).orElseThrow().toSeconds())
                            .maxNumberOfMessages(runContext.render(maxNumberOfMessage).as(Integer.class).orElseThrow())
                            .build();

                        sqsClient.receiveMessage(receiveRequest)
                            .whenComplete((messageResponse, throwable) -> {
                                if (throwable != null) {
                                    fluxSink.error(throwable);
                                } else {
                                    messageResponse.messages().forEach(message -> {
                                        fluxSink.next(Message.builder().data(message.body()).build());
                                    });
                                    messageResponse.messages().forEach(message ->
                                        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                                            .queueUrl(renderedQueueUrl)
                                            .receiptHandle(message.receiptHandle())
                                            .build()
                                        )
                                    );
                                }
                            });
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            isActive.set(false); // proactively stop polling
                        }
                    }
                } catch (Throwable e) {
                    fluxSink.error(e);
                } finally {
                    fluxSink.complete();
                    this.waitForTermination.countDown();
                }
            });
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
}

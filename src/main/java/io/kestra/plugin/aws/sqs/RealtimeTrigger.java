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

    private String queueUrl;

    private String accessKeyId;

    private String secretKeyId;

    private String sessionToken;

    private Property<String> region;

    private String endpointOverride;

    @Builder.Default
    @PluginProperty
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private SerdeType serdeType = SerdeType.STRING;

    // Configuration for AWS STS AssumeRole
    protected String stsRoleArn;
    protected String stsRoleExternalId;
    protected String stsRoleSessionName;
    protected String stsEndpointOverride;
    @Builder.Default
    protected Duration stsRoleSessionDuration = AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION;

    // Default read timeout is 20s, so we cannot use a bigger wait time, or we would need to increase the read timeout.
    @PluginProperty
    @Schema(title = "The duration for which the SQS client waits for a message.")
    @Builder.Default
    protected Duration waitTime = Duration.ofSeconds(20);

    @PluginProperty
    @Schema(
        title = "The maximum number of messages returned from request made to SQS.",
        description = "Increasing this value can reduce the number of requests made to SQS. Amazon SQS never returns more messages than this value (however, fewer messages might be returned). Valid values: 1 to 10."
    )
    @Builder.Default
    protected Integer maxNumberOfMessage = 5;

    @PluginProperty
    @Schema(
        title = "The maximum number of attempts used by the SQS client's retry strategy."
    )
    @Builder.Default
    protected Integer clientRetryMaxAttempts = 3;

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
            .queueUrl(runContext.render(queueUrl))
            .accessKeyId(runContext.render(accessKeyId))
            .secretKeyId(runContext.render(secretKeyId))
            .sessionToken(runContext.render(sessionToken))
            .region(region)
            .endpointOverride(runContext.render(endpointOverride))
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
        var queueUrl = runContext.render(getQueueUrl());

        return Flux.create(
            fluxSink -> {
                try (SqsAsyncClient sqsClient = task.asyncClient(runContext, clientRetryMaxAttempts)) {
                    while (isActive.get()) {
                        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .waitTimeSeconds((int)waitTime.toSeconds())
                            .maxNumberOfMessages(maxNumberOfMessage)
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
                                            .queueUrl(queueUrl)
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

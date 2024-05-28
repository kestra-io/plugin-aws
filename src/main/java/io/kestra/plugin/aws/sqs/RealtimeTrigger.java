package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
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
    title = "Consume a message in real-time from an SQS queue and create one execution per message."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "queueUrl: \"https://sqs.eu-central-1.amazonaws.com/000000000000/test-queue\""
            }
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Message>, SqsConnectionInterface {

    private String queueUrl;

    private String accessKeyId;

    private String secretKeyId;

    private String sessionToken;

    private String region;

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
            .region(runContext.render(region))
            .endpointOverride(runContext.render(endpointOverride))
            .serdeType(this.serdeType)
            .stsRoleArn(this.stsRoleArn)
            .stsRoleSessionName(this.stsRoleSessionName)
            .stsRoleExternalId(this.stsRoleExternalId)
            .stsRoleSessionDuration(this.stsRoleSessionDuration)
            .stsEndpointOverride(this.stsEndpointOverride)
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map(record -> TriggerService.generateRealtimeExecution(this, context, record));
    }

    public Flux<Message> publisher(final Consume task,
                                   final RunContext runContext) throws Exception {
        var queueUrl = runContext.render(getQueueUrl());

        return Flux.create(
            fluxSink -> {
                try (SqsAsyncClient sqsClient = task.asyncClient(runContext)) {
                    while (isActive.get()) {
                        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                            .queueUrl(queueUrl)
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

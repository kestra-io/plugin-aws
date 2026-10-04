package io.kestra.plugin.aws.sqs;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.shared.AbstractConnectionInterface;
import io.kestra.plugin.aws.sqs.model.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on SQS messages (batch polling)",
    description = "Polls a queue on an interval and creates an execution when messages are fetched, stopping at maxRecords or maxDuration. Messages are stored at trigger.uri; autoDelete controls deletion. For per-message realtime, use RealtimeTrigger."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: sqs
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.data }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.aws.sqs.Trigger
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    queueUrl: "https://sqs.eu-central-1.amazonaws.com/000000000000/test-queue"
                    maxRecords: 10
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, SqsConnectionInterface {

    @Schema(title = "Queue url")
    private Property<String> queueUrl;

    @Schema(title = "Access key id")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    private Property<String> accessKeyId;

    @Schema(title = "Secret key id")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    private Property<String> secretKeyId;

    @Schema(title = "Session token")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    private Property<String> sessionToken;

    @Schema(title = "Region")
    private Property<String> region;

    @Schema(title = "Endpoint override")
    private Property<String> endpointOverride;

    @Builder.Default
    @Schema(title = "Max concurrency")
    private Property<Integer> maxConcurrency = Property.ofValue(50);

    @Builder.Default
    @Schema(title = "Connection acquisition timeout")
    private Property<Duration> connectionAcquisitionTimeout = Property.ofValue(Duration.ofSeconds(5));

    @Builder.Default
    @Schema(title = "Interval")
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Max records",
        description = "Stop after consuming this many messages."
    )
    @PluginProperty(group = "execution")
    private Property<Integer> maxRecords;

    @Schema(
        title = "Max duration",
        description = "Stop after this duration elapses."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration;

    @Builder.Default
    @NotNull
    @Schema(
        title = "Serde type",
        description = "Serializer/deserializer used for message bodies."
    )
    @PluginProperty(group = "main")
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    // Configuration for AWS STS AssumeRole
    @Schema(title = "Sts role arn")
    protected Property<String> stsRoleArn;
    @Schema(title = "Sts role external id")
    protected Property<String> stsRoleExternalId;
    @Schema(title = "Sts role session name")
    protected Property<String> stsRoleSessionName;
    @Schema(title = "Sts endpoint override")
    protected Property<String> stsEndpointOverride;
    @Builder.Default

    @Schema(title = "Sts role session duration")
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    @Builder.Default
    @Schema(title = "Auto delete")
    private Property<Boolean> autoDelete = Property.ofValue(true);

    @Builder.Default
    @Schema(title = "Visibility timeout")
    private Property<Integer> visibilityTimeout = Property.ofValue(30);

    // in-flight evaluation, so kill() can release the worker thread if the SQS call hangs
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<Future<?>> running = new AtomicReference<>();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Consume task = Consume.builder()
            .queueUrl(queueUrl)
            .accessKeyId(accessKeyId)
            .autoDelete(this.autoDelete)
            .secretKeyId(secretKeyId)
            .sessionToken(sessionToken)
            .region(region)
            .endpointOverride(endpointOverride)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .serdeType(this.serdeType)
            .stsRoleArn(this.stsRoleArn)
            .stsRoleSessionName(this.stsRoleSessionName)
            .stsRoleExternalId(this.stsRoleExternalId)
            .stsRoleSessionDuration(this.stsRoleSessionDuration)
            .stsEndpointOverride(this.stsEndpointOverride)
            .visibilityTimeout(this.visibilityTimeout)
            .build();

        Consume.Output run = runKillable(() -> task.run(runContext));

        if (logger.isDebugEnabled()) {
            logger.debug("Consumed '{}' messaged.", run.getCount());
        }

        if (run.getCount() == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }

    // Runs the poll on a dedicated thread: the AWS SDK ignores thread interrupts, but the
    // waiting worker thread can always be released by cancelling the future.
    private <T> T runKillable(Callable<T> poll) throws Exception {
        // not try-with-resources: ExecutorService.close() waits for the task, which would block on a hung SDK call
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<T> future = executor.submit(poll);
            running.set(future);
            try {
                return future.get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof Exception cause) {
                    throw cause;
                }
                throw e;
            } catch (InterruptedException e) {
                future.cancel(true);
                Thread.currentThread().interrupt();
                throw e;
            }
        } finally {
            running.set(null);
            executor.shutdownNow(); // never wait for a hung SDK call
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        Future<?> future = running.get();
        if (future != null) {
            future.cancel(true);
        }
    }
}

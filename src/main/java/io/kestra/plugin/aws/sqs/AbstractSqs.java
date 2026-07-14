package io.kestra.plugin.aws.sqs;

import java.time.Duration;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.awscore.retry.AwsRetryPolicy;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Shared SQS connection",
    description = "Provides SQS sync/async clients plus queueUrl and concurrency settings."
)
abstract class AbstractSqs extends AbstractConnection implements SqsConnectionInterface {

    private static final Duration RETRY_STRATEGY_BACKOFF_BASE_DELAY = Duration.ofMillis(50);
    private static final Duration RETRY_STRATEGY_BACKOFF_MAX_DELAY = Duration.ofMillis(300);

    private Property<String> queueUrl;

    @Schema(
        title = "Maximum concurrent HTTP connections to SQS",
        description = "Connection pool size for the SQS async client. It caps how many requests to SQS " +
            "can be in flight at once. It does not limit how many messages are consumed or how fast the " +
            "queue is drained. Defaults to 50, matching the AWS SDK default. Works together with " +
            "connectionAcquisitionTimeout, which is how long a caller waits for a free connection."
    )
    @Builder.Default
    private Property<Integer> maxConcurrency = Property.ofValue(50);

    @Schema(
        title = "Timeout for acquiring an HTTP connection from the pool",
        description = "How long the SQS async client waits for a free connection before failing. This " +
            "applies when all maxConcurrency connections are already in use. Defaults to 5 seconds."
    )
    @Builder.Default
    private Property<Duration> connectionAcquisitionTimeout = Property.ofValue(Duration.ofSeconds(5));

    protected SqsClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, SqsClient.builder()).build();
    }

    protected SqsAsyncClient asyncClient(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureAsyncClient(clientConfig, SqsAsyncClient.builder()).build();
    }

    protected SqsAsyncClient asyncClient(final RunContext runContext,
        final int retryMaxAttempts) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);

        SqsAsyncClientBuilder clientBuilder = ConnectionUtils.configureAsyncClient(
            runContext.render(maxConcurrency).as(Integer.class).orElseThrow(),
            runContext.render(connectionAcquisitionTimeout).as(Duration.class).orElseThrow(),
            clientConfig,
            SqsAsyncClient.builder()
        );

        clientBuilder = clientBuilder.overrideConfiguration(
            builder -> builder.retryPolicy(
                AwsRetryPolicy.defaultRetryPolicy()
                    .toBuilder()
                    .numRetries(retryMaxAttempts)
                    .build()
            )
        );
        return clientBuilder.build();
    }
}

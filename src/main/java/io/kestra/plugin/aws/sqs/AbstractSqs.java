package io.kestra.plugin.aws.sqs;

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

import java.time.Duration;

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

    @Builder.Default
    private Property<Integer> maxConcurrency = Property.ofValue(50);

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

        clientBuilder = clientBuilder.overrideConfiguration(builder ->
            builder.retryPolicy(
                AwsRetryPolicy.defaultRetryPolicy()
                    .toBuilder()
                    .numRetries(retryMaxAttempts)
                    .build()
            )
        );
        return clientBuilder.build();
    }
}

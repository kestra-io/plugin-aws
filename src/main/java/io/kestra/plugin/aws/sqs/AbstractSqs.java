package io.kestra.plugin.aws.sqs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
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
abstract class AbstractSqs extends AbstractConnection implements SqsConnectionInterface {

    private static final Duration RETRY_STRATEGY_BACKOFF_BASE_DELAY = Duration.ofMillis(50);
    private static final Duration RETRY_STRATEGY_BACKOFF_MAX_DELAY = Duration.ofMillis(300);

    private String queueUrl;

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
            50,
            Duration.ofSeconds(5),
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

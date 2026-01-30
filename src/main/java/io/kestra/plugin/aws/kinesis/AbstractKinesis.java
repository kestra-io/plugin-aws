package io.kestra.plugin.aws.kinesis;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Shared Kinesis connection",
    description = "Provides Kinesis sync/async clients using standard AWS connection settings."
)
public class AbstractKinesis extends AbstractConnection {
    protected KinesisAsyncClient asyncClient(final RunContext runContext) throws Exception {
        final AwsClientConfig config = awsClientConfig(runContext);
        return ConnectionUtils.configureAsyncClient(config, KinesisAsyncClient.builder()).build();
    }

    protected KinesisClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, KinesisClient.builder()).build();
    }

    public enum IteratorType {
        AT_SEQUENCE_NUMBER,
        AFTER_SEQUENCE_NUMBER,
        TRIM_HORIZON,
        LATEST,
        AT_TIMESTAMP;
    }
}

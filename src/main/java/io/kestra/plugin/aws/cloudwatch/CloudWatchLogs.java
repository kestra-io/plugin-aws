package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class CloudWatchLogs extends AbstractConnection {

    /**
     * Create a CloudWatchLogsClient using the standard Kestra AWS configuration
     * (credentials, region, endpoint overrides, etc.).
     */
    public CloudWatchLogsClient logsClient(final RunContext runContext)
        throws IllegalVariableEvaluationException {

        final AwsClientConfig clientConfig = awsClientConfig(runContext);

        return ConnectionUtils
            .configureSyncClient(clientConfig, CloudWatchLogsClient.builder())
            .build();
    }
}

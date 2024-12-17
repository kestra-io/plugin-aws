package io.kestra.plugin.aws.s3;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.kestra.plugin.aws.ConnectionUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

import java.net.URI;

public interface AbstractS3 extends AbstractConnectionInterface {
    default S3Client client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AbstractConnection.AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, S3Client.builder()).build();
    }

    default S3AsyncClient asyncClient(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AbstractConnection.AwsClientConfig clientConfig = awsClientConfig(runContext);
        if (runContext.render(this.getCompatibilityMode()).as(Boolean.class).orElse(false)) {
            return ConnectionUtils.configureAsyncClient(clientConfig, S3AsyncClient.builder()).build();
        } else {
            S3CrtAsyncClientBuilder s3ClientBuilder = S3AsyncClient.crtBuilder()
                .credentialsProvider(ConnectionUtils.credentialsProvider(clientConfig));

            if (clientConfig.region() != null) {
                s3ClientBuilder.region(Region.of(clientConfig.region()));
            }
            if (clientConfig.endpointOverride() != null) {
                s3ClientBuilder.endpointOverride(URI.create(clientConfig.endpointOverride()));
            }
            return s3ClientBuilder.build();
        }

    }
}

package io.kestra.plugin.aws.s3;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractS3 extends AbstractConnection {

    protected S3Client client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return configureSyncClient(clientConfig, S3Client.builder()).build();
    }

    protected S3AsyncClient asyncClient(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        if (this.getCompatibilityMode()) {
            return configureAsyncClient(clientConfig, S3AsyncClient.builder()).build();
        } else {
            S3CrtAsyncClientBuilder s3ClientBuilder = S3AsyncClient.crtBuilder()
                .credentialsProvider(AbstractConnectionInterface.credentialsProvider(clientConfig));

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

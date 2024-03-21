package io.kestra.plugin.aws;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import jakarta.annotation.Nullable;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractConnection extends Task implements AbstractConnectionInterface {

    protected String region;
    protected String endpointOverride;
    private Boolean compatibilityMode;

    // Configuration for StaticCredentialsProvider
    protected String accessKeyId;
    protected String secretKeyId;
    protected String sessionToken;

    // Configuration for AWS STS AssumeRole
    protected String stsRoleArn;
    protected String stsRoleExternalId;
    protected String stsRoleSessionName;
    protected String stsEndpointOverride;
    @Builder.Default
    protected Duration stsRoleSessionDuration = AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION;

    /**
     * Configures and returns the given {@link AwsSyncClientBuilder}.
     */
    protected <C extends AwsClient, B extends AwsClientBuilder<B, C> & AwsSyncClientBuilder<B, C>> B configureSyncClient(
        final AwsClientConfig clientConfig, final B builder) throws IllegalVariableEvaluationException {

        builder
            // Use the httpClientBuilder to delegate the lifecycle management of the HTTP client to the AWS SDK
            .httpClientBuilder(serviceDefaults -> ApacheHttpClient.builder().build())
            .credentialsProvider(AbstractConnectionInterface.credentialsProvider(clientConfig));

        return configureClient(clientConfig, builder);
    }

    /**
     * Configures and returns the given {@link AwsAsyncClientBuilder}.
     */
    protected <C extends AwsClient, B extends AwsClientBuilder<B, C> & AwsAsyncClientBuilder<B, C>> B configureAsyncClient(
        final AwsClientConfig clientConfig, final B builder) {

        builder.credentialsProvider(AbstractConnectionInterface.credentialsProvider(clientConfig));
        return configureClient(clientConfig, builder);
    }

    /**
     * Configures and returns the given {@link AwsClientBuilder}.
     */
    protected <C extends AwsClient, B extends AwsClientBuilder<B, C>> B configureClient(
        final AwsClientConfig clientConfig, final B builder) {

        builder.credentialsProvider(AbstractConnectionInterface.credentialsProvider(clientConfig));

        if (clientConfig.region() != null) {
            builder.region(Region.of(clientConfig.region()));
        }
        if (clientConfig.endpointOverride() != null) {
            builder.endpointOverride(URI.create(clientConfig.endpointOverride()));
        }
        return builder;
    }

    /**
     * Common AWS Client configuration properties.
     */
    public record AwsClientConfig(
        @Nullable String accessKeyId,
        @Nullable String secretKeyId,
        @Nullable String sessionToken,
        @Nullable String stsRoleArn,
        @Nullable String stsRoleExternalId,
        @Nullable String stsRoleSessionName,
        @Nullable String stsEndpointOverride,
        Duration stsRoleSessionDuration,
        @Nullable String region,
        @Nullable String endpointOverride
    ) {
    }
}

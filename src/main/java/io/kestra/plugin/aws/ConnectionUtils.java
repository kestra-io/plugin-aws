package io.kestra.plugin.aws;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.time.Duration;

public class ConnectionUtils {
    /**
     * Factory method for constructing a new {@link AwsCredentialsProvider} for the given AWS Client config.
     *
     * @param awsClientConfig The AwsClientConfig.
     * @return  a new {@link AwsCredentialsProvider} instance.
     */
    public static AwsCredentialsProvider credentialsProvider(final AbstractConnection.AwsClientConfig awsClientConfig) {

        // StsAssumeRoleCredentialsProvider
        if (StringUtils.isNotEmpty(awsClientConfig.stsRoleArn())) {
            return stsAssumeRoleCredentialsProvider(awsClientConfig);
        }

        // StaticCredentialsProvider
        if (StringUtils.isNotEmpty(awsClientConfig.accessKeyId()) &&
            StringUtils.isNotEmpty(awsClientConfig.secretKeyId())) {
            return staticCredentialsProvider(awsClientConfig);
        }

        // Otherwise, use DefaultCredentialsProvider
        return DefaultCredentialsProvider.builder().build();
    }

    public static StaticCredentialsProvider staticCredentialsProvider(final AbstractConnection.AwsClientConfig awsClientConfig) {
        final AwsCredentials credentials;
        if (StringUtils.isNotEmpty(awsClientConfig.sessionToken())) {
            credentials = AwsSessionCredentials.create(
                awsClientConfig.accessKeyId(),
                awsClientConfig.secretKeyId(),
                awsClientConfig.sessionToken()
            );
        } else {
            credentials = AwsBasicCredentials.create(
                awsClientConfig.accessKeyId(),
                awsClientConfig.secretKeyId()
            );
        }
        return StaticCredentialsProvider.create(credentials);
    }

    public static StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider(final AbstractConnection.AwsClientConfig awsClientConfig) {

        String roleSessionName = awsClientConfig.stsRoleSessionName();
        roleSessionName = roleSessionName != null ? roleSessionName : "kestra-plugin-s3-" + System.currentTimeMillis();

        final AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
            .roleArn(awsClientConfig.stsRoleArn())
            .roleSessionName(roleSessionName)
            .durationSeconds((int) awsClientConfig.stsRoleSessionDuration().toSeconds())
            .externalId(awsClientConfig.stsRoleExternalId())
            .build();

        return StsAssumeRoleCredentialsProvider.builder()
            .stsClient(stsClient(awsClientConfig))
            .refreshRequest(assumeRoleRequest)
            .build();
    }

    public static StsClient stsClient(final AbstractConnection.AwsClientConfig awsClientConfig) {
        StsClientBuilder builder = StsClient.builder();

        final String stsEndpointOverride = awsClientConfig.stsEndpointOverride();
        if (stsEndpointOverride != null) {
            builder.applyMutation(stsClientBuilder ->
                stsClientBuilder.endpointOverride(URI.create(stsEndpointOverride)));
        }

        final String regionString = awsClientConfig.region();
        if (regionString != null) {
            builder.applyMutation(stsClientBuilder ->
                stsClientBuilder.region(Region.of(regionString)));
        }
        return builder.build();
    }

    /**
     * Configures and returns the given {@link AwsSyncClientBuilder}.
     */
    public static <C extends AwsClient, B extends AwsClientBuilder<B, C> & AwsSyncClientBuilder<B, C>> B configureSyncClient(
        final AbstractConnection.AwsClientConfig clientConfig, final B builder) throws IllegalVariableEvaluationException {

        builder
            // Use the httpClientBuilder to delegate the lifecycle management of the HTTP client to the AWS SDK
            .httpClientBuilder(serviceDefaults -> ApacheHttpClient.builder().build())
            .credentialsProvider(ConnectionUtils.credentialsProvider(clientConfig));

        return configureClient(clientConfig, builder);
    }

    /**
     * Configures and returns the given {@link AwsAsyncClientBuilder}.
     */
    public static <C extends AwsClient, B extends AwsClientBuilder<B, C> & AwsAsyncClientBuilder<B, C>> B configureAsyncClient(
        final AbstractConnection.AwsClientConfig clientConfig, final B builder) {

        builder
            // Use the httpClientBuilder to delegate the lifecycle management of the HTTP client to the AWS SDK
            .httpClientBuilder(serviceDefaults -> NettyNioAsyncHttpClient.builder().build())
            .credentialsProvider(ConnectionUtils.credentialsProvider(clientConfig));
        return configureClient(clientConfig, builder);
    }

    /**
     * Configures and returns the given {@link AwsAsyncClientBuilder}.
     */
    public static <C extends AwsClient, B extends AwsClientBuilder<B, C> & AwsAsyncClientBuilder<B, C>> B configureAsyncClient(
        final int maxConcurrency,
        final Duration connectionAcquisitionTimeout,
        final AbstractConnection.AwsClientConfig clientConfig, final B builder) {

        builder
            // Use the httpClientBuilder to delegate the lifecycle management of the HTTP client to the AWS SDK
            .httpClientBuilder(serviceDefaults -> NettyNioAsyncHttpClient
                .builder()
                .maxConcurrency(maxConcurrency)
                .connectionAcquisitionTimeout(connectionAcquisitionTimeout)
                .build()
            )
            .credentialsProvider(ConnectionUtils.credentialsProvider(clientConfig));
        return configureClient(clientConfig, builder);
    }

    /**
     * Configures and returns the given {@link AwsClientBuilder}.
     */
    public static <C extends AwsClient, B extends AwsClientBuilder<B, C>> B configureClient(
        final AbstractConnection.AwsClientConfig clientConfig, final B builder) {

        builder.credentialsProvider(ConnectionUtils.credentialsProvider(clientConfig));

        if (clientConfig.region() != null) {
            builder.region(Region.of(clientConfig.region()));
        }
        if (clientConfig.endpointOverride() != null) {
            builder.endpointOverride(URI.create(clientConfig.endpointOverride()));
        }
        return builder;
    }
}

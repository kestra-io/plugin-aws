package io.kestra.plugin.aws;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;

import org.apache.commons.lang3.StringUtils;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;

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

public class ConnectionUtils {
    /**
     * Factory method for constructing a new {@link AwsCredentialsProvider} for the given AWS Client config.
     *
     * @param awsClientConfig The AwsClientConfig.
     * @return a new {@link AwsCredentialsProvider} instance.
     */
    public static AwsCredentialsProvider credentialsProvider(final AbstractConnection.AwsClientConfig awsClientConfig) {

        // StsAssumeRoleCredentialsProvider
        if (StringUtils.isNotEmpty(awsClientConfig.stsRoleArn())) {
            return stsAssumeRoleCredentialsProvider(awsClientConfig);
        }

        // StaticCredentialsProvider
        if (
            StringUtils.isNotEmpty(awsClientConfig.accessKeyId()) &&
                StringUtils.isNotEmpty(awsClientConfig.secretKeyId())
        ) {
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
        if (StringUtils.isNotBlank(stsEndpointOverride)) {
            URI stsEndpointUri = URI.create(stsEndpointOverride.trim());
            rejectLinkLocalMetadataHost(stsEndpointUri);
            builder.applyMutation(stsClientBuilder -> stsClientBuilder.endpointOverride(stsEndpointUri));
        }

        final String regionString = awsClientConfig.region();
        if (regionString != null) {
            builder.applyMutation(stsClientBuilder -> stsClientBuilder.region(Region.of(regionString)));
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
            .httpClientBuilder(
                serviceDefaults -> ApacheHttpClient.builder()
                    // 100 balances burst headroom against file-descriptor exhaustion under concurrent executions.
                    .maxConnections(100)
                    .connectionTimeout(Duration.ofSeconds(10))
                    .build()
            )
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
            .httpClientBuilder(
                serviceDefaults -> NettyNioAsyncHttpClient
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

        if (StringUtils.isNotBlank(clientConfig.endpointOverride())) {
            URI endpointOverride = URI.create(clientConfig.endpointOverride().trim());
            rejectLinkLocalMetadataHost(endpointOverride);
            builder.endpointOverride(endpointOverride);
        }
        return builder;
    }

    /**
     * Rejects endpoint overrides that resolve to the 169.254.0.0/16 link-local range, which hosts
     * cloud provider instance-metadata services (e.g. AWS IMDS at 169.254.169.254). Blocking only this
     * range prevents SSRF-based credential exfiltration while still allowing legitimate self-hosted
     * S3-compatible endpoints on RFC-1918 private addresses (e.g. on-prem MinIO).
     */
    public static void rejectLinkLocalMetadataHost(final URI endpointOverride) {
        String host = endpointOverride.getHost();
        if (host == null) {
            return;
        }
        try {
            InetAddress address = InetAddress.getByName(host);
            byte[] octets = address.getAddress();
            if (octets.length == 4 && (octets[0] & 0xFF) == 169 && (octets[1] & 0xFF) == 254) {
                throw new IllegalArgumentException(
                    "Endpoint override '" + endpointOverride + "' resolves to a link-local address (169.254.0.0/16), " +
                        "which is used by cloud instance-metadata services and is not allowed for security reasons."
                );
            }
        } catch (UnknownHostException e) {
            // Host doesn't resolve; let the AWS SDK surface the connection error downstream.
        }
    }
}

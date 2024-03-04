package io.kestra.plugin.aws;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

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

    protected AwsCredentialsProvider credentials(final RunContext runContext) throws IllegalVariableEvaluationException {

        final AwsClientConfig awsClientConfig = awsClientConfig(runContext);

        // StsAssumeRoleCredentialsProvider
        if (StringUtils.isNotEmpty(awsClientConfig.stsRoleArn)) {
            return stsAssumeRoleCredentialsProvider(awsClientConfig);
        }

        // StaticCredentialsProvider
        if (StringUtils.isNotEmpty(awsClientConfig.accessKeyId) &&
            StringUtils.isNotEmpty(awsClientConfig.secretKeyId)) {
            return staticCredentialsProvider(awsClientConfig);
        }

        // Otherwise, use DefaultCredentialsProvider
        return DefaultCredentialsProvider.builder().build();
    }

    private static StaticCredentialsProvider staticCredentialsProvider(final AwsClientConfig awsClientConfig) {
        final AwsCredentials credentials;
        if (StringUtils.isNotEmpty(awsClientConfig.sessionToken())) {
            credentials = AwsSessionCredentials.create(
                awsClientConfig.accessKeyId,
                awsClientConfig.secretKeyId,
                awsClientConfig.sessionToken
            );
        } else {
            credentials = AwsBasicCredentials.create(
                awsClientConfig.accessKeyId,
                awsClientConfig.secretKeyId
            );
        }
        return StaticCredentialsProvider.create(credentials);
    }

    private static StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider(final AwsClientConfig awsClientConfig) {

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

    private static StsClient stsClient(final AwsClientConfig awsClientConfig) {
        StsClientBuilder builder = StsClient.builder();

        final String stsEndpointOverride = awsClientConfig.stsEndpointOverride;
        if (stsEndpointOverride != null) {
            builder.applyMutation(stsClientBuilder ->
                stsClientBuilder.endpointOverride(URI.create(stsEndpointOverride)));
        }

        final String regionString = awsClientConfig.region;
        if (regionString != null) {
            builder.applyMutation(stsClientBuilder ->
                stsClientBuilder.region(Region.of(regionString)));
        }
        return builder.build();
    }

    private AwsClientConfig awsClientConfig(final RunContext runContext) throws IllegalVariableEvaluationException {
        return new AwsClientConfig(
            runContext.render(this.accessKeyId),
            runContext.render(this.secretKeyId),
            runContext.render(this.sessionToken),
            runContext.render(this.stsRoleArn),
            runContext.render(this.stsRoleExternalId),
            runContext.render(this.stsRoleSessionName),
            runContext.render(this.stsEndpointOverride),
            stsRoleSessionDuration,
            runContext.render(this.region),
            runContext.render(this.endpointOverride)
        );
    }

    /**
     * Common AWS Client configuration properties.
     */
    private record AwsClientConfig(
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

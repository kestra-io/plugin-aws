package io.kestra.plugin.aws;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

public interface AbstractConnectionInterface {

    Duration AWS_MIN_STS_ROLE_SESSION_DURATION = Duration.ofSeconds(900);

    @Schema(
        title = "Access Key Id in order to connect to AWS.",
        description = "If no connection is defined, we will use the `DefaultCredentialsProvider` to fetch the value."
    )
    @PluginProperty(dynamic = true)
    String getAccessKeyId();

    @Schema(
        title = "Secret Key Id in order to connect to AWS.",
        description = "If no connection is defined, we will use the `DefaultCredentialsProvider` to fetch the value."
    )
    @PluginProperty(dynamic = true)
    String getSecretKeyId();

    @Schema(
        title = "AWS session token, retrieved from an AWS token service, used for authenticating that this user has received temporary permissions to access a given resource.",
        description = "If no connection is defined, we will use the `DefaultCredentialsProvider` to fetch the value."
    )
    @PluginProperty(dynamic = true)
    String getSessionToken();

    @Schema(
        title =  "AWS STS Role.",
        description = "The Amazon Resource Name (ARN) of the role to assume. If set the task will use the `StsAssumeRoleCredentialsProvider`. Otherwise, the `StaticCredentialsProvider` will be used with the provided Access Key Id and Secret Key."
    )
    @PluginProperty(dynamic = true)

    String getStsRoleArn();

    @Schema(
        title =  "AWS STS External Id.",
        description = " A unique identifier that might be required when you assume a role in another account. This property is only used when an `stsRoleArn` is defined."
    )
    @PluginProperty(dynamic = true)

    String getStsRoleExternalId();
    @Schema(
        title = "AWS STS Session name. This property is only used when an `stsRoleArn` is defined."
    )
    @PluginProperty(dynamic = true)
    String getStsRoleSessionName();

    @Schema(
        title = "AWS STS Session duration.",
        description = "The duration of the role session (default: 15 minutes, i.e., PT15M). This property is only used when an `stsRoleArn` is defined."
    )
    @PluginProperty
    Duration getStsRoleSessionDuration();

    @Schema(
        title = "The AWS STS endpoint with which the SDKClient should communicate."
    )
    @PluginProperty(dynamic = true)
    String getStsEndpointOverride();

    @Schema(
        title = "AWS region with which the SDK should communicate."
    )
    @PluginProperty(dynamic = true)
    String getRegion();

    @Schema(
        title = "The endpoint with which the SDK should communicate.",
        description = "This property should normally not be used except for local development."
    )
    @PluginProperty(dynamic = true)
    String getEndpointOverride();

    static String renderStringConfig(final RunContext runContext, final String config) throws IllegalVariableEvaluationException {
        return Optional.ofNullable(config).map(Rethrow.throwFunction(runContext::render)).orElse(null);
    }

    default AbstractConnection.AwsClientConfig awsClientConfig(final RunContext runContext) throws IllegalVariableEvaluationException {
        return new AbstractConnection.AwsClientConfig(
            renderStringConfig(runContext, this.getAccessKeyId()),
            renderStringConfig(runContext, this.getSecretKeyId()),
            renderStringConfig(runContext, this.getSessionToken()),
            renderStringConfig(runContext, this.getStsRoleArn()),
            renderStringConfig(runContext, this.getStsRoleExternalId()),
            renderStringConfig(runContext, this.getStsRoleSessionName()),
            renderStringConfig(runContext, this.getStsEndpointOverride()),
            getStsRoleSessionDuration(),
            renderStringConfig(runContext, this.getRegion()),
            renderStringConfig(runContext, this.getEndpointOverride())
        );
    }

    /**
     * Factory method for constructing a new {@link AwsCredentialsProvider} for the given AWS Client config.
     *
     * @param awsClientConfig The AwsClientConfig.
     * @return  a new {@link AwsCredentialsProvider} instance.
     */
    static AwsCredentialsProvider credentialsProvider(final AbstractConnection.AwsClientConfig awsClientConfig) {

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

    static StaticCredentialsProvider staticCredentialsProvider(final AbstractConnection.AwsClientConfig awsClientConfig) {
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

    static StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider(final AbstractConnection.AwsClientConfig awsClientConfig) {

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

    static StsClient stsClient(final AbstractConnection.AwsClientConfig awsClientConfig) {
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
}

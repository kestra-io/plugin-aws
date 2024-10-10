package io.kestra.plugin.aws;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

public interface AbstractConnectionInterface {

    Duration AWS_MIN_STS_ROLE_SESSION_DURATION = Duration.ofSeconds(900);

    @Schema(
        title = "Access Key Id in order to connect to AWS.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    @PluginProperty(dynamic = true)
    String getAccessKeyId();

    @Schema(
        title = "Secret Key Id in order to connect to AWS.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    @PluginProperty(dynamic = true)
    String getSecretKeyId();

    @Schema(
        title = "AWS session token, retrieved from an AWS token service, used for authenticating that this user has received temporary permissions to access a given resource.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    @PluginProperty(dynamic = true)
    String getSessionToken();

    @Schema(
        title = "AWS STS Role.",
        description = "The Amazon Resource Name (ARN) of the role to assume. If set the task will use the `StsAssumeRoleCredentialsProvider`. If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    @PluginProperty(dynamic = true)
    String getStsRoleArn();

    @Schema(
        title = "AWS STS External Id.",
        description = " A unique identifier that might be required when you assume a role in another account. This property is only used when an `stsRoleArn` is defined."
    )
    @PluginProperty(dynamic = true)
    String getStsRoleExternalId();

    @Schema(
        title = "AWS STS Session name.",
        description = "This property is only used when an `stsRoleArn` is defined."
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
    Property<String> getRegion();

    @Schema(
        title = "The endpoint with which the SDK should communicate.",
        description = "This property allows you to use a different S3 compatible storage backend."
    )
    @PluginProperty(dynamic = true)
    String getEndpointOverride();

    @PluginProperty(dynamic = true)
    default Boolean getCompatibilityMode() {
        return false;
    }

    default AbstractConnection.AwsClientConfig awsClientConfig(final RunContext runContext) throws IllegalVariableEvaluationException {
        return new AbstractConnection.AwsClientConfig(
            runContext.render(this.getAccessKeyId()),
            runContext.render(this.getSecretKeyId()),
            runContext.render(this.getSessionToken()),
            runContext.render(this.getStsRoleArn()),
            runContext.render(this.getStsRoleExternalId()),
            runContext.render(this.getStsRoleSessionName()),
            runContext.render(this.getStsEndpointOverride()),
            getStsRoleSessionDuration(),
            this.getRegion() == null ? null : this.getRegion().as(runContext, String.class),
            runContext.render(this.getEndpointOverride())
        );
    }
}

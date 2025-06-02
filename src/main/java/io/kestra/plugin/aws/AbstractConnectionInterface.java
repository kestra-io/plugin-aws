package io.kestra.plugin.aws;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
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
    Property<String> getAccessKeyId();

    @Schema(
        title = "Secret Key Id in order to connect to AWS.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    Property<String> getSecretKeyId();

    @Schema(
        title = "AWS session token, retrieved from an AWS token service, used for authenticating that this user has received temporary permissions to access a given resource.",
        description = "If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    Property<String> getSessionToken();

    @Schema(
        title = "AWS STS Role.",
        description = "The Amazon Resource Name (ARN) of the role to assume. If set the task will use the `StsAssumeRoleCredentialsProvider`. If no credentials are defined, we will use the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) to fetch credentials."
    )
    Property<String> getStsRoleArn();

    @Schema(
        title = "AWS STS External Id.",
        description = " A unique identifier that might be required when you assume a role in another account. This property is only used when an `stsRoleArn` is defined."
    )
    Property<String> getStsRoleExternalId();

    @Schema(
        title = "AWS STS Session name.",
        description = "This property is only used when an `stsRoleArn` is defined."
    )
    Property<String> getStsRoleSessionName();

    @Schema(
        title = "AWS STS Session duration.",
        description = "The duration of the role session (default: 15 minutes, i.e., PT15M). This property is only used when an `stsRoleArn` is defined."
    )
    Property<Duration> getStsRoleSessionDuration();

    @Schema(
        title = "The AWS STS endpoint with which the SDKClient should communicate."
    )
    Property<String> getStsEndpointOverride();

    @Schema(
        title = "AWS region with which the SDK should communicate."
    )
    Property<String> getRegion();

    @Schema(
        title = "The endpoint with which the SDK should communicate.",
        description = "This property allows you to use a different S3 compatible storage backend."
    )
    Property<String> getEndpointOverride();

    @Schema(
        title = "Enable compatibility mode.",
        description = "Use it to connect to S3 bucket with S3 compatible services that don't support the new transport client."
    )
    default Property<Boolean> getCompatibilityMode() {
        return Property.of(false);
    }

    @Schema(
        title = "Force path style access.",
        description = "Must only be used when `compatibilityMode` is enabled."
    )
    default Property<Boolean> getForcePathStyle()  {
        return Property.of(false);
    }

    default AbstractConnection.AwsClientConfig awsClientConfig(final RunContext runContext) throws IllegalVariableEvaluationException {
        return new AbstractConnection.AwsClientConfig(
            runContext.render(this.getAccessKeyId()).as(String.class).orElse(null),
            runContext.render(this.getSecretKeyId()).as(String.class).orElse(null),
            runContext.render(this.getSessionToken()).as(String.class).orElse(null),
            runContext.render(this.getStsRoleArn()).as(String.class).orElse(null),
            runContext.render(this.getStsRoleExternalId()).as(String.class).orElse(null),
            runContext.render(this.getStsRoleSessionName()).as(String.class).orElse(null),
            runContext.render(this.getStsEndpointOverride()).as(String.class).orElse(null),
            runContext.render(this.getStsRoleSessionDuration()).as(Duration.class).orElse(null),
            runContext.render(this.getRegion()).as(String.class).orElse(null),
            runContext.render(this.getEndpointOverride()).as(String.class).orElse(null)
        );
    }
}

package io.kestra.plugin.aws;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import jakarta.annotation.Nullable;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractConnection extends Task implements AbstractConnectionInterface {

    protected Property<String> region;
    protected Property<String> endpointOverride;
    protected Property<Boolean> compatibilityMode;
    protected Property<Boolean> forcePathStyle;

    // Configuration for StaticCredentialsProvider
    protected Property<String> accessKeyId;
    protected Property<String> secretKeyId;
    protected Property<String> sessionToken;

    // Configuration for AWS STS AssumeRole
    protected Property<String> stsRoleArn;
    protected Property<String> stsRoleExternalId;
    protected Property<String> stsRoleSessionName;
    protected Property<String> stsEndpointOverride;
    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

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

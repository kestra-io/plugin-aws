package io.kestra.plugin.aws.billing;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.plugin.aws.shared.AbstractConnection;
import io.kestra.plugin.aws.shared.AbstractConnectionInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractBillingTrigger extends AbstractTrigger implements PollingTriggerInterface, AbstractConnectionInterface {
    @Schema(title = "AWS access key ID", description = "Optional static credential. If omitted, the default credentials provider chain is used.")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    protected Property<String> accessKeyId;

    @Schema(title = "AWS secret access key", description = "Pairs with `accessKeyId` for static credentials.")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    protected Property<String> secretKeyId;

    @Schema(title = "AWS session token", description = "Session token for temporary credentials.")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    protected Property<String> sessionToken;

    @Schema(title = "AWS region", description = "Cost Explorer and AWS Budgets are global services served from `us-east-1`.")
    @PluginProperty(group = "connection")
    protected Property<String> region;

    @Schema(title = "Endpoint override", description = "Override the default AWS endpoint URL.")
    @PluginProperty(group = "advanced")
    protected Property<String> endpointOverride;

    @Schema(title = "STS role ARN", description = "IAM role ARN to assume via STS before making API calls.")
    @PluginProperty(group = "advanced")
    protected Property<String> stsRoleArn;

    @Schema(title = "STS role external ID", description = "External ID to pass when assuming the STS role.")
    @PluginProperty(group = "advanced")
    protected Property<String> stsRoleExternalId;

    @Schema(title = "STS role session name", description = "Session name to use when assuming the STS role.")
    @PluginProperty(group = "advanced")
    protected Property<String> stsRoleSessionName;

    @Schema(title = "STS endpoint override", description = "Override the default STS endpoint URL.")
    @PluginProperty(group = "advanced")
    protected Property<String> stsEndpointOverride;

    @Schema(title = "STS role session duration", description = "Duration of the assumed role session. Default PT15M.")
    @PluginProperty(group = "execution")
    @Builder.Default
    protected Property<Duration> stsRoleSessionDuration = Property.ofValue(AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION);

    /**
     * Copies this trigger's connection properties to the builder of the billing task it runs.
     */
    protected void copyConnection(AbstractConnection.AbstractConnectionBuilder<?, ?> builder) {
        builder.accessKeyId(accessKeyId)
            .secretKeyId(secretKeyId)
            .sessionToken(sessionToken)
            .region(region)
            .endpointOverride(endpointOverride)
            .stsRoleArn(stsRoleArn)
            .stsRoleExternalId(stsRoleExternalId)
            .stsRoleSessionName(stsRoleSessionName)
            .stsEndpointOverride(stsEndpointOverride)
            .stsRoleSessionDuration(stsRoleSessionDuration);
    }

    @VisibleForTesting
    Instant now() {
        return Instant.now();
    }

    LocalDate today() {
        return LocalDate.ofInstant(now(), ZoneOffset.UTC);
    }
}

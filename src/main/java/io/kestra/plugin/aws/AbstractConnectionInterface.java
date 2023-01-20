package io.kestra.plugin.aws;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public interface AbstractConnectionInterface {
    @Schema(
        title = "The Access Key Id in order to connect to AWS",
        description = "If no connection is defined, we will use default DefaultCredentialsProvider that will try to guess the value"
    )
    @PluginProperty(dynamic = true)
    String getAccessKeyId();

    @Schema(
        title = "The Secret Key Id in order to connect to AWS",
        description = "If no connection is defined, we will use default DefaultCredentialsProvider that will try to guess the value"
    )
    @PluginProperty(dynamic = true)
    String getSecretKeyId();

    @Schema(
        title = "The he AWS session token, retrieved from an AWS token service, used for authenticating that this user has received temporary permission to access some resource.",
        description = "If no connection is defined, we will use default DefaultCredentialsProvider that will try to guess the value"
    )
    @PluginProperty(dynamic = true)
    String getSessionToken();

    @Schema(
        title = "The region with which the SDK should communicate."
    )
    @PluginProperty(dynamic = true)
    String getRegion();

    @Schema(
        title = "The endpoint with which the SDK should communicate.",
        description = "Should normally not be used except for local development."
    )
    @PluginProperty(dynamic = true)
    String getEndpointOverride();
}

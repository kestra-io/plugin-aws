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
}

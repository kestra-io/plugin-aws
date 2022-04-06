package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public interface AbstractS3Interface {
    @Schema(
        title = "The AWS region to used"
    )
    @PluginProperty(dynamic = true)
    String getRegion();

    @Schema(
        title = "The Secret Key Id in order to connect to AWS",
        description = "If no connection is defined, we will use default DefaultCredentialsProvider that will try to guess the value"
    )
    @PluginProperty(dynamic = true)
    String getEndpointOverride();
}

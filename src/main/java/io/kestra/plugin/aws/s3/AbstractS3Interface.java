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

    @Schema(
        title = "Option to enable using path style access for accessing S3 objects instead of DNS style access.",
        description = "DNS style access is preferred as it will result in better load balancing when accessing S3.\n" +
            "Path style access is disabled by default. Path style may still be used for legacy buckets that are not DNS compatible."
    )
    @PluginProperty(dynamic = false)
    Boolean getPathStyleAccess();
}

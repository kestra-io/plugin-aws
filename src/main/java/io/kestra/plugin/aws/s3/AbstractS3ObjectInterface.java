package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public interface AbstractS3ObjectInterface {
    @Schema(
        title = "The S3 bucket name."
    )
    @PluginProperty(dynamic = true)
    String getBucket();

    @Schema(
        title = "Sets the value of the RequestPayer property for this object."
    )
    @PluginProperty(dynamic = true)
    String getRequestPayer();
}

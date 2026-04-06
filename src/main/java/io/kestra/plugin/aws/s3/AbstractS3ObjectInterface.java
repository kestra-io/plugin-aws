package io.kestra.plugin.aws.s3;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.PluginProperty;

public interface AbstractS3ObjectInterface extends AbstractS3 {
    @Schema(
        title = "The S3 bucket name."
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<String> getBucket();

    @Schema(
        title = "Sets the value of the RequestPayer property for this object."
    )
    @PluginProperty(group = "advanced")
    Property<String> getRequestPayer();
}

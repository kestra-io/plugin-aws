package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.PluginProperty;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@NoArgsConstructor
public abstract class ObjectOutput {
    @Schema(
        title = "An ETag is an opaque identifier assigned by a web server to a specific version of a resource found at a URL"
    )
    @PluginProperty(group = "advanced")
    private String eTag;

    @Schema(
        title = "The version of the object"
    )
    @PluginProperty(group = "advanced")
    private String versionId;
}

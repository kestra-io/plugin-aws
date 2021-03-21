package io.kestra.plugin.aws.s3;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
public abstract class ObjectOutput {
    @Schema(
        title = "An ETag is an opaque identifier assigned by a web server to a specific version of a resource found at a URL."
    )
    private final String eTag;

    @Schema(
        title = "Version of the object."
    )
    private final String versionId;
}

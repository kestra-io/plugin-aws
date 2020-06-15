package org.kestra.task.aws.s3;

import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.OutputProperty;

@SuperBuilder
@Getter
public abstract class ObjectOutput {
    @OutputProperty(
        description = "An ETag is an opaque identifier assigned by a web server to a specific version of a resource found at a URL."
    )
    private final String eTag;

    @OutputProperty(
        description = "Version of the object."
    )
    private final String versionId;
}

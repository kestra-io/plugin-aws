package io.kestra.plugin.aws.s3;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.aws.AbstractConnection;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractS3Object extends AbstractConnection implements AbstractS3ObjectInterface {
    protected Property<String> requestPayer;

    protected Property<String> bucket;

    static {
        // Initializing CRT will download the S3 native library into /tmp.
        // With Java Security enabled, we need to do it early so it is done out of a task execution.
        S3Service.initCrt();
    }
}

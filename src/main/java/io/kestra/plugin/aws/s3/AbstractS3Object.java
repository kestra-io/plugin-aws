package io.kestra.plugin.aws.s3;

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
    protected String requestPayer;

    protected String bucket;
}

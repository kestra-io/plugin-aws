package io.kestra.plugin.aws.s3;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractS3Object extends AbstractS3 {
    @Schema(
        title = "Sets the value of the RequestPayer property for this object."
    )
    @PluginProperty(dynamic = true)
    protected String requestPayer;

}

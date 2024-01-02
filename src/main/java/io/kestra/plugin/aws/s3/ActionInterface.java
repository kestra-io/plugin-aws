package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface ActionInterface {
    @Schema(
        title = "The action to perform on the retrieved files."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    ActionInterface.Action getAction();

    @Schema(
        title = "The destination bucket and key."
    )
    @PluginProperty(dynamic = true)
    Copy.CopyObject getMoveTo();

    enum Action {
        MOVE,
        DELETE
    }
}

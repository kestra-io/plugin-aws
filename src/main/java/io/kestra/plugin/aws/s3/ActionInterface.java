package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface ActionInterface {
    @Schema(
        title = "The action to perform on the retrieved files. If using 'NONE' make sure to handle the files inside your flow to avoid infinite triggering."
    )
    @NotNull
    Property<Action> getAction();

    @Schema(
        title = "The destination bucket and key for `MOVE` action."
    )
    @PluginProperty(dynamic = true)
    Copy.CopyObject getMoveTo();

    enum Action {
        MOVE,
        DELETE,
        NONE
    }
}

package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface ActionInterface {
    @Schema(
        title = "The action to perform on the retrieved files. If using 'NONE' make sure to handle the files inside your flow to avoid infinite triggering"
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<Action> getAction();

    @Schema(
        title = "The destination bucket and key for `MOVE` action",
        description = """
            When moving files, any relative directory structure under the configured `prefix` is preserved under `moveTo.key`.
            If the `prefix` does not end on a folder boundary (e.g. `data/2023`), the common parent folder (`data/`) is used to preserve subdirectories.
            If no `prefix` is configured, only the file name is appended.
            If multiple objects resolve to the same destination key, execution fails before any copy or delete occurs to prevent data loss.
            """
    )
    @PluginProperty(dynamic = true, group = "destination")
    Copy.CopyObject getMoveTo();

    enum Action {
        MOVE,
        DELETE,
        NONE
    }
}

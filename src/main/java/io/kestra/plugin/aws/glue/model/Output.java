package io.kestra.plugin.aws.glue.model;

import java.time.ZonedDateTime;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import io.kestra.core.models.annotations.PluginProperty;

@Builder
@Getter
public class Output implements io.kestra.core.models.tasks.Output {
    @Schema(
        title = "Job name"
    )
    @PluginProperty(group = "advanced")
    private final String jobName;

    @Schema(
        title = "Job run ID"
    )
    @PluginProperty(group = "advanced")
    private final String jobRunId;

    @Schema(
        title = "Job state",
        description = "Current state string, e.g., SUCCEEDED or RUNNING."
    )
    @PluginProperty(group = "advanced")
    private final String state;

    @Schema(
        title = "Started on",
        description = "Start timestamp."
    )
    @PluginProperty(group = "advanced")
    private final ZonedDateTime startedOn;

    @Schema(
        title = "Completed on",
        description = "Completion timestamp when available."
    )
    @PluginProperty(group = "advanced")
    private final ZonedDateTime completedOn;

    @Schema(
        title = "Last modified on",
        description = "Last modification timestamp."
    )
    @PluginProperty(group = "advanced")
    private final ZonedDateTime lastModifiedOn;

    @Schema(
        title = "Execution time (s)",
        description = "Execution duration in seconds reported by Glue."
    )
    @PluginProperty(group = "advanced")
    private final Integer executionTime;

    @Schema(
        title = "Timeout (min)",
        description = "Configured timeout in minutes."
    )
    @PluginProperty(group = "execution")
    private final Integer timeout;

    @Schema(
        title = "Attempt",
        description = "Attempt number for this run."
    )
    @PluginProperty(group = "advanced")
    private final Integer attempt;

    @Schema(
        title = "Error message",
        description = "Error message when present."
    )
    @PluginProperty(group = "advanced")
    private final String errorMessage;
}

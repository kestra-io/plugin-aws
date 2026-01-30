package io.kestra.plugin.aws.glue.model;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.time.ZonedDateTime;

@Builder
@Getter
public class Output implements io.kestra.core.models.tasks.Output {
    @Schema(
        title = "Job name"
    )
    private final String jobName;

    @Schema(
        title = "Job run ID"
    )
    private final String jobRunId;

    @Schema(
        title = "Job state",
        description = "Current state string, e.g., SUCCEEDED or RUNNING."
    )
    private final String state;

    @Schema(
        title = "Started on",
        description = "Start timestamp."
    )
    private final ZonedDateTime startedOn;

    @Schema(
        title = "Completed on",
        description = "Completion timestamp when available."
    )
    private final ZonedDateTime completedOn;

    @Schema(
        title = "Last modified on",
        description = "Last modification timestamp."
    )
    private final ZonedDateTime lastModifiedOn;

    @Schema(
        title = "Execution time (s)",
        description = "Execution duration in seconds reported by Glue."
    )
    private final Integer executionTime;

    @Schema(
        title = "Timeout (min)",
        description = "Configured timeout in minutes."
    )
    private final Integer timeout;

    @Schema(
        title = "Attempt",
        description = "Attempt number for this run."
    )
    private final Integer attempt;

    @Schema(
        title = "Error message",
        description = "Error message when present."
    )
    private final String errorMessage;
}

package io.kestra.plugin.aws.glue.model;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.time.ZonedDateTime;

@Builder
@Getter
public class Output implements io.kestra.core.models.tasks.Output {
    @Schema(title = "The name of the job")
    private final String jobName;

    @Schema(title = "The ID of the job run")
    private final String jobRunId;

    @Schema(title = "The current state of the job run")
    private final String state;

    @Schema(title = "When the job run was started")
    private final ZonedDateTime startedOn;

    @Schema(title = "When the job run was completed, if applicable")
    private final ZonedDateTime completedOn;

    @Schema(title = "The last time the job run was modified")
    private final ZonedDateTime lastModifiedOn;

    @Schema(title = "The execution time of the job in seconds")
    private final Integer executionTime;

    @Schema(title = "The timeout configuration for the job in minutes")
    private final Integer timeout;

    @Schema(title = "The attempt number for this job run")
    private final Integer attempt;

    @Schema(title = "The error message if the job failed")
    private final String errorMessage;
}

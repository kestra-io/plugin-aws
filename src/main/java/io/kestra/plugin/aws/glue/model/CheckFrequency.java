package io.kestra.plugin.aws.glue.model;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
public class CheckFrequency {
    @Schema(
        title = "Maximum time (in seconds) to wait for job completion",
        description = "Job will fail if it exceeds this duration",
        defaultValue = "3600"
    )
    @Builder.Default
    private Integer maxDuration = 3600;

    @Schema(
        title = "Polling interval (in seconds) between status checks",
        description = "Time to wait between each status check",
        defaultValue = "30"
    )
    @Builder.Default
    private Integer interval = 30;

    @Schema(
        title = "Maximum number of status checks",
        description = "Job will fail if the number of checks exceeds this value",
        defaultValue = "120"
    )
    @Builder.Default
    private Integer maxIterations = 120;
}
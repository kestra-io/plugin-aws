package io.kestra.plugin.aws.sqs.model;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import javax.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class Message {
    @Schema(title = "The message data.")
    @PluginProperty(dynamic = true)
    @NotNull
    private String data;

    @Schema(title = "The message group ID.")
    @PluginProperty(dynamic = true)
    private String groupId;

    @Schema(title = "The message deduplication ID.")
    @PluginProperty(dynamic = true)
    private String deduplicationId;

    @Schema(title = "The message delay in seconds.")
    @PluginProperty
    private Integer delaySeconds;

    public SendMessageRequest to(SendMessageRequest.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        return builder
            .messageBody(runContext.render(data))
            .messageGroupId(runContext.render(groupId))
            .messageDeduplicationId(runContext.render(deduplicationId))
            .delaySeconds(delaySeconds)
            .build();
    }
}

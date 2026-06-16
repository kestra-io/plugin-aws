package io.kestra.plugin.aws.sqs.model;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@Getter
@Builder
@Jacksonized
public class Message implements io.kestra.core.models.tasks.Output {
    @Schema(
        title = "The message data.",
        description = """
            Accepts a string (Pebble-templated, e.g. "{{ inputs.payload }}") or a structured value \
            (map, list) that is serialized to JSON before sending. \
            String values are rendered at runtime; non-string values are passed through Jackson.\
            """
    )
    @PluginProperty(group = "main")
    @NotNull
    private Object data;

    @Schema(title = "The message group ID.")
    @PluginProperty(dynamic = true, group = "advanced")
    private String groupId;

    @Schema(title = "The message deduplication ID.")
    @PluginProperty(dynamic = true, group = "advanced")
    private String deduplicationId;

    @Schema(title = "The message delay in seconds.")
    @PluginProperty(group = "advanced")
    private Integer delaySeconds;

    public SendMessageRequest to(SendMessageRequest.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return builder
            .messageBody(toMessageBody(runContext))
            .messageGroupId(runContext.render(groupId))
            .messageDeduplicationId(runContext.render(deduplicationId))
            .delaySeconds(delaySeconds)
            .build();
    }

    public SendMessageBatchRequestEntry toBatchEntry(String id, RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return SendMessageBatchRequestEntry.builder()
            .id(id)
            .messageBody(toMessageBody(runContext))
            .messageGroupId(runContext.render(groupId))
            .messageDeduplicationId(runContext.render(deduplicationId))
            .delaySeconds(delaySeconds)
            .build();
    }

    private String toMessageBody(RunContext runContext) throws IllegalVariableEvaluationException, JsonProcessingException {
        if (data instanceof String s) {
            return runContext.render(s);
        }
        return JacksonMapper.ofJson(false).writeValueAsString(data);
    }
}

package io.kestra.plugin.aws.sns.model;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import javax.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class Message {
    @Schema(title = "The message data")
    @PluginProperty(dynamic = true)
    @NotNull
    private String data;

    @Schema(title = "The message group ID")
    @PluginProperty(dynamic = true)
    private String groupId;

    @Schema(title = "The message deduplication ID")
    @PluginProperty(dynamic = true)
    private String deduplicationId;

    @Schema(title = "The message subject")
    @PluginProperty(dynamic = true)
    private String subject;

    @Schema(title = "The message phone number")
    @PluginProperty(dynamic = true)
    private String phoneNumber;

    @Schema(title = "The message structure")
    @PluginProperty(dynamic = true)
    private String structure;


    public PublishRequest to(PublishRequest.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        return builder
            .message(runContext.render(data))
            .messageGroupId(runContext.render(groupId))
            .messageDeduplicationId(runContext.render(deduplicationId))
            .subject(runContext.render(subject))
            .phoneNumber(runContext.render(phoneNumber))
            .messageStructure(runContext.render(structure))
            .build();
    }
}

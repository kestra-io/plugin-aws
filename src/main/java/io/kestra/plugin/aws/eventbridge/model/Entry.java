package io.kestra.plugin.aws.eventbridge.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Getter
@Builder
@EqualsAndHashCode
@Jacksonized
public class Entry {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();

    @Schema(title = "The name or ARN of the event bus to receive the event.")
    @PluginProperty(dynamic = true)
    @NotNull
    @JsonAlias("EventBusName")
    private String eventBusName;

    @Schema(title = "The source of the event.")
    @PluginProperty(dynamic = true)
    @NotNull
    @JsonAlias("Source")
    private String source;

    @Schema(title = "Free-form string used to decide what fields to expect in the event detail.")
    @PluginProperty(dynamic = true)
    @NotNull
    @JsonAlias("DetailType")
    private String detailType;

    @Schema(
        title = "The EventBridge entry.",
        description = "Can be a JSON string, or a map."
    )
    @PluginProperty(dynamic = true)
    @JsonAlias("Detail")
    private Object detail;

    @Schema(
        title = "AWS resources which the event primarily concerns.",
        description = "AWS resources, identified by Amazon Resource Name (ARN), which the event primarily concerns. Any number, including zero, may be present."
    )
    @PluginProperty(dynamic = true)
    @JsonAlias("Resources")
    private List<String> resources;

    public PutEventsRequestEntry toRequestEntry(RunContext runContext) throws IllegalVariableEvaluationException, JsonProcessingException {
        var source = runContext.render(this.source);
        var busName = runContext.render(eventBusName);
        var type = runContext.render(detailType);
        var json = jsonValue(runContext, detail);

        return PutEventsRequestEntry.builder()
            .eventBusName(busName)
            .source(source)
            .detailType(type)
            .detail(json)
            .resources(resources)
            .build();
    }

    @SuppressWarnings("unchecked")
    private String jsonValue(RunContext runContext, Object event) throws IllegalVariableEvaluationException, JsonProcessingException {
        if (event instanceof String) {
            return runContext.render((String) event);
        } else if (event instanceof Map) {
            return OBJECT_MAPPER.writeValueAsString(runContext.render((Map<String, Object>) event));
        }

        throw new IllegalVariableEvaluationException("Invalid event type '" + event.getClass() + "'");
    }
}

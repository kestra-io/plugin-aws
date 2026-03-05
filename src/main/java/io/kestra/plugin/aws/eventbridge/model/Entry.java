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

import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Getter
@Builder
@EqualsAndHashCode
@Jacksonized
public class Entry {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();

    @Schema(
        title = "Event bus",
        description = "Name or ARN of the event bus that receives the event."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @JsonAlias("EventBusName")
    private String eventBusName;

    @Schema(
        title = "Source",
        description = "Event source identifier, typically a domain or app name."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @JsonAlias("Source")
    private String source;

    @Schema(
        title = "Detail type",
        description = "Classifier used by rules to interpret the detail payload."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @JsonAlias("DetailType")
    private String detailType;

    @Schema(
        title = "Detail payload",
        description = "Event detail as JSON string or map; rendered then serialized to JSON."
    )
    @PluginProperty(dynamic = true)
    @JsonAlias("Detail")
    private Object detail;

    @Schema(
        title = "Resources",
        description = "Optional list of ARNs the event primarily concerns."
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

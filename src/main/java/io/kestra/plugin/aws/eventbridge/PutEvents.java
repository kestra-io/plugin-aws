package io.kestra.plugin.aws.eventbridge;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.kestra.plugin.aws.eventbridge.model.Entry;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Send multiple custom events as maps to Amazon EventBridge so that they can be matched to rules.",
            full = true,
            code = """
                id: aws_event_bridge_put_events
                namespace: company.team

                tasks:
                  - id: put_events
                    type: io.kestra.plugin.aws.eventbridge.PutEvents
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    entries:
                      - eventBusName: "events"
                        source: "Kestra"
                        detailType: "my_object"
                        detail:
                          message: "hello from EventBridge and Kestra"
                """
        ),
        @Example(
            title = "Send multiple custom events as a JSON string to Amazon EventBridge so that they can be matched to rules.",
            full = true,
            code = """
                id: aws_event_bridge_put_events
                namespace: company.team

                tasks:
                  - id: put_events
                    type: io.kestra.plugin.aws.eventbridge.PutEvents
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    entries:
                      - eventBusName: "events"
                        source: "Kestra"
                        detailType: "my_object"
                        detail: "{\"message\": \"hello from EventBridge and Kestra\"}"
                        resources:
                          - "arn:aws:iam::123456789012:user/johndoe"
                """
        )
    }
)
@Schema(
    title = "Send custom events to Amazon EventBridge for rule matching."
)
public class PutEvents extends AbstractConnection implements RunnableTask<PutEvents.Output> {
    private static final ObjectMapper MAPPER = JacksonMapper.ofIon()
        .setSerializationInclusion(JsonInclude.Include.ALWAYS);

    @NotNull
    @Schema(
        title = "Mark the task as failed when sending an event is unsuccessful.",
        description = "If true, the task will fail when any event fails to be sent."
    )
    @Builder.Default
    private Property<Boolean> failOnUnsuccessfulEvents = Property.ofValue(true);

    @PluginProperty(dynamic = true)
    @NotNull
    @Schema(
        title = "List of event entries to send to, or internal storage URI to retrieve it.",
        description = "A list of at least one EventBridge entry.",
        oneOf = {String.class, Entry[].class}
    )
    private Object entries;

    @Override
    public PutEvents.Output run(RunContext runContext) throws Exception {
        final long start = System.nanoTime();

        List<Entry> entryList = readEntryList(runContext, entries);

        PutEventsResponse putEventsResponse = putEvents(runContext, entryList);

        // Set metrics
        runContext.metric(Timer.of("duration", Duration.ofNanos(System.nanoTime() - start)));
        runContext.metric(Counter.of("failedEntryCount", putEventsResponse.failedEntryCount()));
        runContext.metric(Counter.of("successfulEntryCount", entryList.size() - putEventsResponse.failedEntryCount()));
        runContext.metric(Counter.of("entryCount", entryList.size()));

        // Fail if failOnUnsuccessfulEvents
        if (runContext.render(failOnUnsuccessfulEvents).as(Boolean.class).orElseThrow() && putEventsResponse.failedEntryCount() > 0) {
            var logger = runContext.logger();
            logger.error("Response show {} event failed: {}", putEventsResponse.failedEntryCount(), putEventsResponse);
            throw new RuntimeException(String.format("Response show %d event failed: %s", putEventsResponse.failedEntryCount(), putEventsResponse));
        }

        File tempFile = writeOutputFile(runContext, putEventsResponse, entryList);
        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .failedEntryCount(putEventsResponse.failedEntryCount())
            .entryCount(entryList.size())
            .build();
    }

    private File writeOutputFile(RunContext runContext, PutEventsResponse putEventsResponse, List<Entry> entryList) throws IOException {
        // Create Output
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var stream = new FileOutputStream(tempFile)) {
            List<PutEventsResultEntry> responseEntries = putEventsResponse.entries();
            for (int i = 0; i < responseEntries.size(); i++) {
                PutEventsResultEntry responseEntry = responseEntries.get(i);
                OutputEntry entry = OutputEntry.builder()
                    .entry(entryList.get(i))
                    .eventId(responseEntry.eventId())
                    .errorCode(responseEntry.errorCode())
                    .errorMessage(responseEntry.errorMessage())
                    .build();
                FileSerde.write(stream, entry);
            }
        }
        return tempFile;
    }

    private PutEventsResponse putEvents(RunContext runContext, List<Entry> entryList) throws Exception {
        try (var eventBridgeClient = client(runContext)) {
            List<PutEventsRequestEntry> requestEntries = entryList.stream()
                .map(throwFunction(entry -> entry.toRequestEntry(runContext)))
                .collect(Collectors.toList());

            PutEventsRequest eventsRequest = PutEventsRequest.builder()
                .entries(requestEntries)
                .build();

            return eventBridgeClient.putEvents(eventsRequest);
        }
    }

    private EventBridgeClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, EventBridgeClient.builder()).build();
    }

    private List<Entry> readEntryList(RunContext runContext, Object entries) throws IllegalVariableEvaluationException, URISyntaxException, IOException {
        if (entries instanceof String) {
            URI from = new URI(runContext.render((String) entries));
            if (!from.getScheme().equals("kestra")) {
                throw new IllegalArgumentException("Invalid entries parameter, must be a Kestra internal storage URI, or a list of entries.");
            }
            try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)))) {
                return FileSerde.readAll(inputStream, Entry.class)
                    .collectList().block();
            }
        } else if (entries instanceof List) {
            return MAPPER.convertValue(entries, new TypeReference<>() {
            });
        }

        throw new IllegalVariableEvaluationException("Invalid event type '" + entries.getClass() + "'");
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The URI of the stored data.",
            description = "The successfully and unsuccessfully ingested events. " +
                "If the ingestion was successful, the entry has the event ID in it. " +
                "Otherwise, you can use the error code and error message to identify the problem with the entry."
        )
        private URI uri;

        @Schema(
            title = "The number of failed entries."
        )
        private int failedEntryCount;

        @Schema(
            title = "The total number of entries."
        )
        private int entryCount;

        @Override
        public Optional<State.Type> finalState() {
            return this.failedEntryCount > 0 ? Optional.of(State.Type.WARNING) : io.kestra.core.models.tasks.Output.super.finalState();
        }
    }

    @Builder
    @Getter
    public static class OutputEntry {
        @Schema(
            title = "The ID of the event."
        )
        private final String eventId;

        @Schema(
            title = "The error code that indicates why the event submission failed."
        )
        private final String errorCode;

        @Schema(
            title = "The error message that explains why the event submission failed."
        )
        private final String errorMessage;

        @Schema(
            title = "The original entry."
        )
        private final Entry entry;
    }
}

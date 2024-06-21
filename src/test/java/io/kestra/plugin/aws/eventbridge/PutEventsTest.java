package io.kestra.plugin.aws.eventbridge;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.kestra.plugin.aws.eventbridge.model.Entry;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Testcontainers
class PutEventsTest extends AbstractLocalStackTest {

    @Inject
    protected RunContextFactory runContextFactory;
    @Inject
    protected StorageInterface storageInterface;

    private static List<PutEvents.OutputEntry> getOutputEntries(PutEvents put, RunContext runContext) throws Exception {
        var output = put.run(runContext);
        List<PutEvents.OutputEntry> outputEntries;
        URI from = output.getUri();
        if (!from.getScheme().equals("kestra")) {
            throw new IllegalArgumentException("Invalid entries parameter, must be a Kestra internal storage URI, or a list of entry.");
        }
        try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)))) {
            outputEntries = Flux.create(FileSerde.reader(inputStream, PutEvents.OutputEntry.class), FluxSink.OverflowStrategy.BUFFER).collectList().block();
        }
        return outputEntries;
    }

    @Test
    void runMap() throws Exception {
        var runContext = runContextFactory.of();

        Entry entry = Entry.builder()
            .source("Kestra")
            .detailType("hello")
            .detail(Map.of(
                "details", "hello from kestra",
                "firstname", "John",
                "lastname", "Doe"
            ))
            .build();
        Entry entry2 = Entry.builder()
            .source("Kestra")
            .detailType("hello")
            .detail(Map.of(
                "details", "hello from kestra 2",
                "firstname", "John",
                "lastname", "Doe"
            ))
            .build();
        Entry entry3 = Entry.builder()
            .source("Kestra")
            .detailType("hello")
            .detail(Map.of(
                "details", "hello from kestra 3",
                "firstname", "John",
                "lastname", "Doe"
            ))
            .build();
        var put = PutEvents.builder()
            .endpointOverride(localstack.getEndpoint().toString())
            .region(localstack.getRegion())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .entries(List.of(entry, entry2, entry3))
            .build();


        List<PutEvents.OutputEntry> outputEntries = getOutputEntries(put, runContext);
        assertThat(outputEntries, hasSize(3));
        assertThat(outputEntries.get(0).getEventId(), notNullValue());
        assertThat(outputEntries.get(0).getErrorCode(), nullValue());
        assertThat(outputEntries.get(0).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(0).getEntry(), equalTo(entry));

        assertThat(outputEntries.get(1).getEventId(), notNullValue());
        assertThat(outputEntries.get(1).getErrorCode(), nullValue());
        assertThat(outputEntries.get(1).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(1).getEntry(), equalTo(entry2));

        assertThat(outputEntries.get(2).getEventId(), notNullValue());
        assertThat(outputEntries.get(2).getErrorCode(), nullValue());
        assertThat(outputEntries.get(2).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(2).getEntry(), equalTo(entry3));
    }

    @Test
    void runStorage() throws Exception {
        var runContext = runContextFactory.of();

        Entry entry = Entry.builder()
            .source("Kestra")
            .detailType("hello")
            .detail(Map.of(
                "details", "hello from kestra",
                "firstname", "John",
                "lastname", "Doe"
            ))
            .build();
        Entry entry2 = Entry.builder()
            .source("Kestra")
            .detailType("hello")
            .detail(Map.of(
                "details", "hello from kestra 2",
                "firstname", "John",
                "lastname", "Doe"
            ))
            .build();
        Entry entry3 = Entry.builder()
            .source("Kestra")
            .detailType("hello")
            .detail(Map.of(
                "details", "hello from kestra 3",
                "firstname", "John",
                "lastname", "Doe"
            ))
            .build();

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var stream = new FileOutputStream(tempFile)) {
            List.of(entry, entry2, entry3).forEach(throwConsumer(e -> FileSerde.write(stream, e)));
        }

        var put = PutEvents.builder()
            .endpointOverride(localstack.getEndpoint().toString())
            .region(localstack.getRegion())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .entries(runContext.storage().putFile(tempFile).toString())
            .build();


        List<PutEvents.OutputEntry> outputEntries = getOutputEntries(put, runContext);

        assertThat(outputEntries, hasSize(3));
        assertThat(outputEntries.get(0).getEventId(), notNullValue());
        assertThat(outputEntries.get(0).getErrorCode(), nullValue());
        assertThat(outputEntries.get(0).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(0).getEntry(), equalTo(entry));

        assertThat(outputEntries.get(1).getEventId(), notNullValue());
        assertThat(outputEntries.get(1).getErrorCode(), nullValue());
        assertThat(outputEntries.get(1).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(1).getEntry(), equalTo(entry2));

        assertThat(outputEntries.get(2).getEventId(), notNullValue());
        assertThat(outputEntries.get(2).getErrorCode(), nullValue());
        assertThat(outputEntries.get(2).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(2).getEntry(), equalTo(entry3));
    }

    @Test
    void runString() throws Exception {
        var runContext = runContextFactory.of();

        Entry entry = Entry.builder()
            .eventBusName("test-bus")
            .source("Kestra")
            .detailType("hello")
            .detail("{\"details\": \"hello from kestra\", \"firstname\": \"Jane\", \"lastname\": \"Doe\"}")
            .resources(List.of(
                "arn:aws:iam::123456789012:user/johndoe",
                "arn:aws:iam::123456789012:user/janeoe"
            ))
            .build();
        var put = PutEvents.builder()
            .endpointOverride(localstack.getEndpoint().toString())
            .region(localstack.getRegion())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .entries(List.of(entry, entry, entry))
            .build();

        List<PutEvents.OutputEntry> outputEntries = getOutputEntries(put, runContext);
        assertThat(outputEntries, hasSize(3));
        assertThat(outputEntries.get(0).getEventId(), notNullValue());
        assertThat(outputEntries.get(0).getErrorCode(), nullValue());
        assertThat(outputEntries.get(0).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(0).getEntry(), equalTo(entry));
    }

    @Test
    void runStringUpperCase() throws Exception {
        var runContext = runContextFactory.of();

        UpperCaseEntry entry = UpperCaseEntry.builder()
            .EventBusName("test-bus")
            .Source("Kestra")
            .DetailType("hello")
            .Detail("{\"details\": \"hello from kestra\", \"firstname\": \"Jane\", \"lastname\": \"Doe\"}")
            .Resources(List.of(
                "arn:aws:iam::123456789012:user/johndoe",
                "arn:aws:iam::123456789012:user/janeoe"
            ))
            .build();
        var put = PutEvents.builder()
            .endpointOverride(localstack.getEndpoint().toString())
            .region(localstack.getRegion())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .entries(List.of(entry, entry, entry))
            .build();

        List<PutEvents.OutputEntry> outputEntries = getOutputEntries(put, runContext);
        assertThat(outputEntries, hasSize(3));
        assertThat(outputEntries.get(0).getEventId(), notNullValue());
        assertThat(outputEntries.get(0).getErrorCode(), nullValue());
        assertThat(outputEntries.get(0).getErrorMessage(), nullValue());
        assertThat(outputEntries.get(0).getEntry().getDetail(), equalTo(entry.Detail));
    }

    /**
     * Test that user can use AWS notation in json
     */
    @Getter
    @Builder
    @EqualsAndHashCode
    @Jacksonized
    private static class UpperCaseEntry {
        private String EventBusName;
        private String Source;
        private String DetailType;
        private Object Detail;
        private List<String> Resources;

    }
}

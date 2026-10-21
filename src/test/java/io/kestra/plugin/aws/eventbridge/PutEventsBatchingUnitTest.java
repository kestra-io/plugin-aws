package io.kestra.plugin.aws.eventbridge;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextProperty;
import io.kestra.core.runners.WorkingDir;
import io.kestra.core.storages.Storage;
import io.kestra.plugin.aws.eventbridge.model.Entry;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PutEventsBatchingUnitTest {

    @Mock
    private RunContext runContext;

    @Mock
    private Storage storage;

    @Mock
    private WorkingDir workingDir;

    @Mock
    private RunContextProperty runContextProperty;

    @Mock
    private EventBridgeClient mockClient;

    @Test
    void testBatchingRequestShapeAndFailureCount() throws Exception {
        var tempFile = File.createTempFile("test-batching", ".ion");
        tempFile.deleteOnExit();

        when(runContext.storage()).thenReturn(storage);
        when(runContext.workingDir()).thenReturn(workingDir);
        when(workingDir.createTempFile(anyString())).thenReturn(tempFile.toPath());
        when(storage.putFile(any(File.class))).thenReturn(URI.create("kestra:///test.ion"));
        when(runContext.render(any(Property.class))).thenReturn(runContextProperty);
        when(runContextProperty.as(Boolean.class)).thenReturn(Optional.of(Boolean.FALSE));

        when(mockClient.putEvents(any(PutEventsRequest.class)))
            .thenReturn(
                PutEventsResponse.builder()
                    .entries(List.of(PutEventsResultEntry.builder().eventId("id-1").build()))
                    .failedEntryCount(1)
                    .build(),
                PutEventsResponse.builder()
                    .entries(List.of(PutEventsResultEntry.builder().eventId("id-2").build()))
                    .failedEntryCount(0)
                    .build()
            );

        var entries = new ArrayList<Entry>();
        for (int i = 0; i < 15; i++) {
            entries.add(
                Entry.builder()
                    .source("custom.test")
                    .detailType("unit-test")
                    .detail("{\"key\": \"val-" + i + "\"}")
                    .build()
            );
        }

        var task = PutEvents.builder()
            .id("unit-test-put-events")
            .type(PutEvents.class.getName())
            .entries(entries)
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .failOnUnsuccessfulEvents(Property.ofValue(false))
            .build();

        var spyTask = spy(task);
        doReturn(mockClient).when(spyTask).client(any());

        var output = spyTask.run(runContext);

        ArgumentCaptor<PutEventsRequest> captor = ArgumentCaptor.forClass(PutEventsRequest.class);
        verify(mockClient, times(2)).putEvents(captor.capture());

        var capturedRequests = captor.getAllValues();
        assertEquals(2, capturedRequests.size());
        assertEquals(10, capturedRequests.get(0).entries().size());
        assertEquals(5, capturedRequests.get(1).entries().size());

        verify(mockClient, times(2)).putEvents(argThat((PutEventsRequest r) -> r.entries().size() <= 10));

        assertEquals(1, output.getFailedEntryCount());
        assertEquals(15, output.getEntryCount());
    }
}

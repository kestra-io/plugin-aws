package io.kestra.plugin.aws.eventbridge;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.eventbridge.model.Entry;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class PutEventsBatchingUnitTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void testBatchingRequestShapeAndFailureCount() throws Exception {
        var runContext = runContextFactory.of();

        var mockClient = mock(EventBridgeClient.class);
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

        var captor = ArgumentCaptor.forClass(PutEventsRequest.class);
        verify(mockClient, times(2)).putEvents(captor.capture());

        var capturedRequests = captor.getAllValues();
        assertEquals(10, capturedRequests.getFirst().entries().size());
        assertEquals(5, capturedRequests.get(1).entries().size());

        assertEquals(1, output.getFailedEntryCount());
        assertEquals(15, output.getEntryCount());
    }
}

package io.kestra.plugin.aws.emr;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.DeleteApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.DeleteApplicationResponse;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DeleteServerlessApplicationTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void runDeletesMultipleApplications() throws Exception {
        var runContext = runContextFactory.of();

        // Mock AWS EMR Serverless client
        EmrServerlessClient client = mock(EmrServerlessClient.class);

        // Stub the consumer overload
        when(client.deleteApplication(any(Consumer.class)))
            .thenReturn(DeleteApplicationResponse.builder().build());

        // Build task and spy it to override client(runContext)
        var task = spy(DeleteServerlessApplication.builder()
            .id("delete_app")
            .type(DeleteServerlessApplication.class.getName())
            .region(Property.ofValue("eu-central-1"))
            .applicationIds(Property.ofValue(List.of("00f123abc456xyz", "11g789def012uvw")))
            .build());

        // Force task to use mocked client instead of real AWS client
        doReturn(client).when(task).client(any());

        var output = task.run(runContext);
        assertThat(output, nullValue()); // RunnableTask<VoidOutput> returns null

        // Capture the Consumer<DeleteApplicationRequest.Builder> arguments
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<DeleteApplicationRequest.Builder>> captor =
            ArgumentCaptor.forClass(Consumer.class);

        verify(client, times(2)).deleteApplication(captor.capture());

        List<Consumer<DeleteApplicationRequest.Builder>> consumers = captor.getAllValues();
        assertThat(consumers, hasSize(2));

        // Apply each consumer to a builder to inspect the resulting request
        DeleteApplicationRequest req1 = buildRequest(consumers.get(0));
        DeleteApplicationRequest req2 = buildRequest(consumers.get(1));

        assertThat(req1.applicationId(), is("00f123abc456xyz"));
        assertThat(req2.applicationId(), is("11g789def012uvw"));

        // Verify that the client is closed (try-with-resources)
        verify(client, times(1)).close();
    }

    @Test
    void runDeletesSingleApplication() throws Exception {
        var runContext = runContextFactory.of();

        // Mock AWS EMR Serverless client
        EmrServerlessClient client = mock(EmrServerlessClient.class);
        when(client.deleteApplication(any(Consumer.class)))
            .thenReturn(DeleteApplicationResponse.builder().build());

        // Build task for a single application deletion
        var task = spy(DeleteServerlessApplication.builder()
            .id("delete_app")
            .type(DeleteServerlessApplication.class.getName())
            .region(Property.ofValue("eu-central-1"))
            .applicationIds(Property.ofValue(List.of("app-123")))
            .build());

        // Force task to use mocked client
        doReturn(client).when(task).client(any());

        task.run(runContext);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<DeleteApplicationRequest.Builder>> captor =
            ArgumentCaptor.forClass(Consumer.class);

        verify(client, times(1)).deleteApplication(captor.capture());

        DeleteApplicationRequest req = buildRequest(captor.getValue());
        assertThat(req.applicationId(), is("app-123"));

        verify(client).close();
    }

    @Test
    void runWithEmptyListDoesNothing() throws Exception {
        var runContext = runContextFactory.of();

        // Mock AWS EMR Serverless client
        EmrServerlessClient client = mock(EmrServerlessClient.class);

        // Build task with an empty list of application IDs
        var task = spy(DeleteServerlessApplication.builder()
            .id("delete_app")
            .type(DeleteServerlessApplication.class.getName())
            .region(Property.ofValue("eu-central-1"))
            .applicationIds(Property.ofValue(List.of()))
            .build());

        // Force task to use mocked client
        doReturn(client).when(task).client(any());

        task.run(runContext);

        // Ensure no delete call is made when list is empty
        verify(client, never()).deleteApplication(any(Consumer.class));

        verify(client).close();
    }

    private DeleteApplicationRequest buildRequest(Consumer<DeleteApplicationRequest.Builder> consumer) {
        // Helper to materialize the request from the captured consumer
        DeleteApplicationRequest.Builder builder = DeleteApplicationRequest.builder();
        consumer.accept(builder);
        return builder.build();
    }
}

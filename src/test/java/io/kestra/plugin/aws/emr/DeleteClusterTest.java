package io.kestra.plugin.aws.emr;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.TerminateJobFlowsRequest;
import software.amazon.awssdk.services.emr.model.TerminateJobFlowsResponse;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DeleteClusterTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void allClusterIdsArePassedInSingleCall() throws Exception {
        RunContext runContext = runContextFactory.of();
        EmrClient emrClient = mock(EmrClient.class);
        when(emrClient.terminateJobFlows(any(Consumer.class)))
            .thenReturn(TerminateJobFlowsResponse.builder().build());

        DeleteCluster task = spy(
            DeleteCluster.builder()
                .region(Property.ofValue("eu-west-3"))
                .clusterIds(Property.ofValue(List.of("j-111", "j-222", "j-333")))
                .build()
        );
        doReturn(emrClient).when(task).emrClient(any(RunContext.class));

        task.run(runContext);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<TerminateJobFlowsRequest.Builder>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(emrClient, times(1)).terminateJobFlows(captor.capture());

        TerminateJobFlowsRequest.Builder builder = TerminateJobFlowsRequest.builder();
        captor.getValue().accept(builder);
        TerminateJobFlowsRequest req = builder.build();

        assertThat(req.jobFlowIds(), hasSize(3));
        assertThat(req.jobFlowIds(), containsInAnyOrder("j-111", "j-222", "j-333"));
    }
}

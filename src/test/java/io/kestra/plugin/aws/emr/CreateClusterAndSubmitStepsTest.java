package io.kestra.plugin.aws.emr;

import java.time.Duration;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class CreateClusterAndSubmitStepsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void waitUntilTerminated() throws Exception {
        RunContext runContext = runContextFactory.of();
        EmrClient emrClient = mock(EmrClient.class);

        when(emrClient.runJobFlow(any(RunJobFlowRequest.class)))
            .thenReturn(RunJobFlowResponse.builder().jobFlowId("j-WAIT").build());

        when(emrClient.describeCluster(any(Consumer.class)))
            .thenReturn(
                DescribeClusterResponse.builder()
                    .cluster(
                        Cluster.builder()
                            .status(ClusterStatus.builder().state(ClusterState.TERMINATED).build())
                            .build()
                    )
                    .build()
            );

        CreateClusterAndSubmitSteps task = spy(
            CreateClusterAndSubmitSteps.builder()
                .region(Property.ofValue("eu-west-3"))
                .clusterName(Property.ofValue("wait-test-cluster"))
                .masterInstanceType(Property.ofValue("m5.xlarge"))
                .slaveInstanceType(Property.ofValue("m5.xlarge"))
                .instanceCount(Property.ofValue(1))
                .wait(Property.ofValue(true))
                .completionCheckInterval(Property.ofValue(Duration.ofMillis(100)))
                .waitUntilCompletion(Property.ofValue(Duration.ofSeconds(10)))
                .build()
        );
        doReturn(emrClient).when(task).emrClient(any(RunContext.class));

        CreateClusterAndSubmitSteps.Output output = task.run(runContext);

        assertNotNull(output);
        assertEquals("j-WAIT", output.getJobFlowId());
        verify(emrClient).runJobFlow(any(RunJobFlowRequest.class));
        verify(emrClient, atLeastOnce()).describeCluster(any(Consumer.class));
    }

    @Test
    void waitUntilTerminatedWithErrors() throws Exception {
        RunContext runContext = runContextFactory.of();
        EmrClient emrClient = mock(EmrClient.class);

        when(emrClient.runJobFlow(any(RunJobFlowRequest.class)))
            .thenReturn(RunJobFlowResponse.builder().jobFlowId("j-ERR").build());

        when(emrClient.describeCluster(any(Consumer.class)))
            .thenReturn(
                DescribeClusterResponse.builder()
                    .cluster(
                        Cluster.builder()
                            .status(ClusterStatus.builder().state(ClusterState.TERMINATED_WITH_ERRORS).build())
                            .build()
                    )
                    .build()
            );

        CreateClusterAndSubmitSteps task = spy(
            CreateClusterAndSubmitSteps.builder()
                .region(Property.ofValue("eu-west-3"))
                .clusterName(Property.ofValue("error-cluster"))
                .masterInstanceType(Property.ofValue("m5.xlarge"))
                .slaveInstanceType(Property.ofValue("m5.xlarge"))
                .instanceCount(Property.ofValue(1))
                .wait(Property.ofValue(true))
                .completionCheckInterval(Property.ofValue(Duration.ofMillis(100)))
                .waitUntilCompletion(Property.ofValue(Duration.ofSeconds(10)))
                .build()
        );
        doReturn(emrClient).when(task).emrClient(any(RunContext.class));

        CreateClusterAndSubmitSteps.Output output = task.run(runContext);

        assertNotNull(output);
        assertEquals("j-ERR", output.getJobFlowId());
    }
}

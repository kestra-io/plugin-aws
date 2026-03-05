package io.kestra.plugin.aws.emr;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.emr.models.StepConfig;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class EmrTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void createCluster_mocked() throws Exception {
        RunContext runContext = runContextFactory.of();

        EmrClient emrClient = mock(EmrClient.class);

        when(emrClient.runJobFlow(any(RunJobFlowRequest.class)))
            .thenReturn(
                RunJobFlowResponse.builder()
                    .jobFlowId("j-123")
                    .build()
            );

        CreateClusterAndSubmitSteps task = spy(CreateClusterAndSubmitSteps.builder()
            .region(Property.ofValue("eu-west-3"))
            .clusterName(Property.ofValue("UNIT_TEST_CLUSTER"))
            .steps(List.of(createPythonSparkJob()))
            .logUri(Property.ofValue("s3://bucket/test-emr-logs"))
            .keepJobFlowAliveWhenNoSteps(Property.ofValue(true))
            .applications(Property.ofValue(List.of("Spark")))
            .masterInstanceType(Property.ofValue("m5.xlarge"))
            .slaveInstanceType(Property.ofValue("m5.xlarge"))
            .instanceCount(Property.ofValue(3))
            .ec2KeyName(Property.ofValue("my-key"))
            .wait(Property.ofValue(false))
            .build());

        doReturn(emrClient).when(task).emrClient(any(RunContext.class));

        CreateClusterAndSubmitSteps.Output output = task.run(runContext);

        assertNotNull(output);
        assertEquals("j-123", output.getJobFlowId());

        verify(emrClient).runJobFlow(any(RunJobFlowRequest.class));
        verify(emrClient, never()).describeCluster(any(DescribeClusterRequest.class));
    }

    @Test
    void deleteCluster_mocked() throws Exception {
        RunContext runContext = runContextFactory.of();

        EmrClient emrClient = mock(EmrClient.class);

        when(emrClient.terminateJobFlows(any(java.util.function.Consumer.class)))
            .thenReturn(TerminateJobFlowsResponse.builder().build());

        DeleteCluster task = spy(DeleteCluster.builder()
            .region(Property.ofValue("eu-west-3"))
            .clusterIds(Property.ofValue(List.of("j-123")))
            .build());

        doReturn(emrClient).when(task).emrClient(any(RunContext.class));

        task.run(runContext);

        verify(emrClient).terminateJobFlows(any(Consumer.class));
    }

    @Test
    void addStepsToCluster_mocked() throws Exception {
        RunContext runContext = runContextFactory.of();

        EmrClient emrClient = mock(EmrClient.class);

        when(emrClient.addJobFlowSteps(any(Consumer.class)))
            .thenReturn(AddJobFlowStepsResponse.builder().stepIds("s-111").build());

        SubmitSteps task = spy(SubmitSteps.builder()
            .region(Property.ofValue("eu-west-3"))
            .clusterId(Property.ofValue("j-123"))
            .steps(List.of(createPythonSparkJob()))
            .build());

        doReturn(emrClient).when(task).emrClient(any(RunContext.class));

        task.run(runContext);

        verify(emrClient).addJobFlowSteps(any(Consumer.class));
    }

    private StepConfig createPythonSparkJob() {
        return StepConfig.builder()
            .jar(Property.ofValue("command-runner.jar"))
            .commands(Property.ofValue(
                List.of("spark-submit s3://bucket/health_violations.py --data_source s3://bucket/data.csv --output_uri s3://bucket/out")
            ))
            .name(Property.ofValue("TEST SPARK JOB UNIT TEST"))
            .actionOnFailure(Property.ofValue(StepConfig.Action.CONTINUE))
            .build();
    }
}

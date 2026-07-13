package io.kestra.plugin.aws.msk;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.ClusterInfo;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.DescribeClusterRequest;
import software.amazon.awssdk.services.kafka.model.DescribeClusterResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class TriggerTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenClusterAtTargetState_whenEvaluate_thenReturnsExecution() throws Exception {
        var trigger = Trigger.builder()
            .id("test-trigger")
            .type(Trigger.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .clusterArn(Property.ofValue("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc"))
            .targetState(Property.ofValue(ClusterState.ACTIVE))
            .build();

        var mockClient = mock(KafkaClient.class);
        when(mockClient.describeCluster(any(DescribeClusterRequest.class)))
            .thenReturn(
                DescribeClusterResponse.builder()
                    .clusterInfo(
                        ClusterInfo.builder()
                            .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc")
                            .state(ClusterState.ACTIVE)
                            .build()
                    )
                    .build()
            );

        var conditionContext = TestsUtils.mockTrigger(runContextFactory, trigger);

        var spy = spy(trigger);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        Optional<Execution> result = spy.evaluate(conditionContext.getKey(), conditionContext.getValue().context());

        assertThat(result.isPresent(), is(true));
    }

    @Test
    void givenClusterNotAtTargetState_whenEvaluate_thenReturnsEmpty() throws Exception {
        var trigger = Trigger.builder()
            .id("test-trigger-not-ready")
            .type(Trigger.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .clusterArn(Property.ofValue("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc"))
            .targetState(Property.ofValue(ClusterState.ACTIVE))
            .build();

        var mockClient = mock(KafkaClient.class);
        when(mockClient.describeCluster(any(DescribeClusterRequest.class)))
            .thenReturn(
                DescribeClusterResponse.builder()
                    .clusterInfo(
                        ClusterInfo.builder()
                            .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc")
                            .state(ClusterState.CREATING)
                            .build()
                    )
                    .build()
            );

        var conditionContext = TestsUtils.mockTrigger(runContextFactory, trigger);

        var spy = spy(trigger);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        Optional<Execution> result = spy.evaluate(conditionContext.getKey(), conditionContext.getValue().context());

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void clusterStateEnum_containsExpectedValues() {
        var expected = new String[] { "ACTIVE", "CREATING", "DELETING", "FAILED", "HEALING", "MAINTENANCE", "REBOOTING_BROKER", "UPDATING" };
        for (var state : expected) {
            assertThat(ClusterState.fromValue(state), not(ClusterState.UNKNOWN_TO_SDK_VERSION));
        }
    }
}

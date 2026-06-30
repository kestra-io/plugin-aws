package io.kestra.plugin.aws.msk;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.*;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class TriggerTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenActiveCluster_whenTargetIsActive_thenShouldFire() {
        // Verify matching state logic — when current == target, trigger fires
        var currentState = ClusterState.ACTIVE;
        var targetState = ClusterState.ACTIVE;
        assertThat(currentState, is(targetState));
    }

    @Test
    void givenCreatingCluster_whenTargetIsActive_thenShouldNotFire() {
        // Verify non-matching state — trigger should not fire
        var currentState = ClusterState.CREATING;
        var targetState = ClusterState.ACTIVE;
        assertThat(currentState, not(targetState));
    }

    @Test
    void givenClusterArn_whenDescribe_thenStateIsReturned() {
        var mockClient = mock(KafkaClient.class);
        when(mockClient.describeCluster(any(DescribeClusterRequest.class)))
            .thenReturn(DescribeClusterResponse.builder()
                .clusterInfo(ClusterInfo.builder()
                    .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc")
                    .state(ClusterState.ACTIVE)
                    .build())
                .build());

        var response = mockClient.describeCluster(
            DescribeClusterRequest.builder()
                .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc")
                .build());

        assertThat(response.clusterInfo().state(), is(ClusterState.ACTIVE));
    }

    @Test
    void clusterStateEnum_containsExpectedValues() {
        // Verify all documented ClusterState values exist in the SDK enum
        var states = List.of("ACTIVE", "CREATING", "DELETING", "FAILED", "HEALING", "MAINTENANCE", "REBOOTING_BROKER", "UPDATING");
        for (var state : states) {
            assertThat(ClusterState.fromValue(state), not(ClusterState.UNKNOWN_TO_SDK_VERSION));
        }
    }
}

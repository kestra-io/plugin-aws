package io.kestra.plugin.aws.msk;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.ClusterInfo;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.DescribeClusterRequest;
import software.amazon.awssdk.services.kafka.model.DescribeClusterResponse;

import java.util.Optional;

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
        var runContext = runContextFactory.of();

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
            .thenReturn(DescribeClusterResponse.builder()
                .clusterInfo(ClusterInfo.builder()
                    .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc")
                    .state(ClusterState.ACTIVE)
                    .build())
                .build());

        var spy = spy(trigger);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var conditionContext = mock(ConditionContext.class);
        var triggerContext = mock(TriggerContext.class);
        when(conditionContext.getRunContext()).thenReturn(runContext);

        Optional<Execution> result = spy.evaluate(conditionContext, triggerContext);

        assertThat(result.isPresent(), is(true));
    }

    @Test
    void givenClusterNotAtTargetState_whenEvaluate_thenReturnsEmpty() throws Exception {
        var runContext = runContextFactory.of();

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
            .thenReturn(DescribeClusterResponse.builder()
                .clusterInfo(ClusterInfo.builder()
                    .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc")
                    .state(ClusterState.CREATING)
                    .build())
                .build());

        var spy = spy(trigger);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var conditionContext = mock(ConditionContext.class);
        var triggerContext = mock(TriggerContext.class);
        when(conditionContext.getRunContext()).thenReturn(runContext);

        Optional<Execution> result = spy.evaluate(conditionContext, triggerContext);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void clusterStateEnum_containsExpectedValues() {
        var expected = new String[]{"ACTIVE", "CREATING", "DELETING", "FAILED", "HEALING", "MAINTENANCE", "REBOOTING_BROKER", "UPDATING"};
        for (var state : expected) {
            assertThat(ClusterState.fromValue(state), not(ClusterState.UNKNOWN_TO_SDK_VERSION));
        }
    }
}

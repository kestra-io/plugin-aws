package io.kestra.plugin.aws.msk;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DeleteClusterTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenClusterArn_whenDelete_thenOutputContainsDeletingState() throws Exception {
        var runContext = runContextFactory.of();

        var task = DeleteCluster.builder()
            .id("test-delete-cluster")
            .type(DeleteCluster.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .clusterArn(Property.ofValue("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc123"))
            .build();

        var describeResponse = DescribeClusterResponse.builder()
            .clusterInfo(ClusterInfo.builder()
                .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc123")
                .currentVersion("K3AEGXETSR30VB")
                .state(ClusterState.ACTIVE)
                .build())
            .build();

        var deleteResponse = DeleteClusterResponse.builder()
            .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc123")
            .state(ClusterState.DELETING)
            .build();

        var mockClient = mock(KafkaClient.class);
        when(mockClient.describeCluster(any(DescribeClusterRequest.class))).thenReturn(describeResponse);
        when(mockClient.deleteCluster(any(DeleteClusterRequest.class))).thenReturn(deleteResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getClusterArn(), containsString("my-cluster"));
        assertThat(output.getState(), is("DELETING"));
    }
}

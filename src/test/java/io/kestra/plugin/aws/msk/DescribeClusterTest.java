package io.kestra.plugin.aws.msk;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.*;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DescribeClusterTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenClusterArn_whenDescribe_thenOutputContainsMetadata() throws Exception {
        var runContext = runContextFactory.of();

        var task = DescribeCluster.builder()
            .id("test-describe-cluster")
            .type(DescribeCluster.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .clusterArn(Property.ofValue("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc123"))
            .build();

        var info = ClusterInfo.builder()
            .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc123")
            .clusterName("my-cluster")
            .state(ClusterState.ACTIVE)
            .currentVersion("K3AEGXETSR30VB")
            .numberOfBrokerNodes(3)
            .currentBrokerSoftwareInfo(BrokerSoftwareInfo.builder().kafkaVersion("3.5.1").build())
            .zookeeperConnectString("z-1.my-cluster.abc123.c1.kafka.us-east-1.amazonaws.com:2181")
            .creationTime(Instant.parse("2024-01-01T00:00:00Z"))
            .build();

        var mockResponse = DescribeClusterResponse.builder()
            .clusterInfo(info)
            .build();

        var mockClient = mock(KafkaClient.class);
        when(mockClient.describeCluster(any(DescribeClusterRequest.class))).thenReturn(mockResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getClusterName(), is("my-cluster"));
        assertThat(output.getState(), is("ACTIVE"));
        assertThat(output.getKafkaVersion(), is("3.5.1"));
        assertThat(output.getNumberOfBrokerNodes(), is(3));
        assertThat(output.getZookeeperConnectString(), containsString("2181"));
        assertThat(output.getCreationTime(), notNullValue());
    }
}

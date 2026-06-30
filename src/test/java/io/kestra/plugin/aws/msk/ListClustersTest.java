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
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class ListClustersTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenNoFilter_whenList_thenReturnsAllClusters() throws Exception {
        var runContext = runContextFactory.of();

        var task = ListClusters.builder()
            .id("test-list-clusters")
            .type(ListClusters.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .build();

        var cluster1 = ClusterInfo.builder()
            .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/cluster-one/abc")
            .clusterName("cluster-one")
            .state(ClusterState.ACTIVE)
            .numberOfBrokerNodes(3)
            .currentBrokerSoftwareInfo(BrokerSoftwareInfo.builder().kafkaVersion("3.5.1").build())
            .creationTime(Instant.parse("2024-01-01T00:00:00Z"))
            .build();

        var cluster2 = ClusterInfo.builder()
            .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/cluster-two/def")
            .clusterName("cluster-two")
            .state(ClusterState.CREATING)
            .numberOfBrokerNodes(6)
            .currentBrokerSoftwareInfo(BrokerSoftwareInfo.builder().kafkaVersion("3.6.0").build())
            .creationTime(Instant.parse("2024-02-01T00:00:00Z"))
            .build();

        var mockResponse = ListClustersResponse.builder()
            .clusterInfoList(List.of(cluster1, cluster2))
            .build();

        var mockClient = mock(KafkaClient.class);
        when(mockClient.listClusters(any(ListClustersRequest.class))).thenReturn(mockResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getTotal(), is(2));
        assertThat(output.getClusters(), hasSize(2));
        assertThat(output.getClusters().get(0).get("clusterName"), is("cluster-one"));
        assertThat(output.getClusters().get(0).get("state"), is("ACTIVE"));
        assertThat(output.getClusters().get(1).get("kafkaVersion"), is("3.6.0"));
    }
}

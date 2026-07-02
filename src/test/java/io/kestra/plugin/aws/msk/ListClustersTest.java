package io.kestra.plugin.aws.msk;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.BrokerSoftwareInfo;
import software.amazon.awssdk.services.kafka.model.ClusterInfo;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.ListClustersRequest;
import software.amazon.awssdk.services.kafka.model.ListClustersResponse;
import software.amazon.awssdk.core.exception.SdkException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
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

        var mockClient = mock(KafkaClient.class);
        when(mockClient.listClusters(any(ListClustersRequest.class)))
            .thenReturn(ListClustersResponse.builder()
                .clusterInfoList(List.of(cluster1, cluster2))
                .build());

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getTotal(), is(2));
        assertThat(output.getClusters(), hasSize(2));
        assertThat(output.getClusters().get(0).get("clusterName"), is("cluster-one"));
        assertThat(output.getClusters().get(0).get("state"), is("ACTIVE"));
        assertThat(output.getClusters().get(1).get("kafkaVersion"), is("3.6.0"));
    }

    @Test
    void givenNameFilter_whenList_thenFilterIsPassedToRequest() throws Exception {
        var runContext = runContextFactory.of();

        var task = ListClusters.builder()
            .id("test-list-clusters-filter")
            .type(ListClusters.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .clusterNameFilter(Property.ofValue("prod-cluster"))
            .build();

        var mockClient = mock(KafkaClient.class);
        when(mockClient.listClusters(any(ListClustersRequest.class)))
            .thenReturn(ListClustersResponse.builder()
                .clusterInfoList(List.of(ClusterInfo.builder()
                    .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/prod-cluster/abc")
                    .clusterName("prod-cluster")
                    .state(ClusterState.ACTIVE)
                    .numberOfBrokerNodes(3)
                    .build()))
                .build());

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getTotal(), is(1));
        // Verify the filter was passed to the SDK request
        verify(mockClient).listClusters(argThat((ListClustersRequest req) ->
            "prod-cluster".equals(req.clusterNameFilter())));
    }

    @Test
    void givenMultiplePages_whenList_thenAggregatesResults() throws Exception {
        var runContext = runContextFactory.of();

        var task = ListClusters.builder()
            .id("test-list-clusters-pages")
            .type(ListClusters.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .build();

        var page1Cluster = ClusterInfo.builder()
            .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/cluster-page1/abc")
            .clusterName("cluster-page1")
            .state(ClusterState.ACTIVE)
            .numberOfBrokerNodes(3)
            .build();

        var page2Cluster = ClusterInfo.builder()
            .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/cluster-page2/def")
            .clusterName("cluster-page2")
            .state(ClusterState.CREATING)
            .numberOfBrokerNodes(6)
            .build();

        var mockClient = mock(KafkaClient.class);
        when(mockClient.listClusters(argThat((ListClustersRequest req) -> req.nextToken() == null)))
            .thenReturn(ListClustersResponse.builder()
                .clusterInfoList(List.of(page1Cluster))
                .nextToken("page2-token")
                .build());
        when(mockClient.listClusters(argThat((ListClustersRequest req) -> "page2-token".equals(req.nextToken()))))
            .thenReturn(ListClustersResponse.builder()
                .clusterInfoList(List.of(page2Cluster))
                .build());

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getTotal(), is(2));
        assertThat(output.getClusters().get(0).get("clusterName"), is("cluster-page1"));
        assertThat(output.getClusters().get(1).get("clusterName"), is("cluster-page2"));
        verify(mockClient, times(2)).listClusters(any(ListClustersRequest.class));
    }

    @Test
    void givenSdkException_whenList_thenExceptionPropagates() throws Exception {
        var runContext = runContextFactory.of();

        var task = ListClusters.builder()
            .id("test-list-clusters-error")
            .type(ListClusters.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .build();

        var mockClient = mock(KafkaClient.class);
        when(mockClient.listClusters(any(ListClustersRequest.class)))
            .thenThrow(SdkException.create("Service unavailable", new RuntimeException()));

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        assertThrows(SdkException.class, () -> spy.run(runContext));
    }
}

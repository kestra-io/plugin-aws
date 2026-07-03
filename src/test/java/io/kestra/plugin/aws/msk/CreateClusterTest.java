package io.kestra.plugin.aws.msk;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
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
class CreateClusterTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenValidConfig_whenCreate_thenOutputContainsArn() throws Exception {
        var runContext = runContextFactory.of();

        var task = CreateCluster.builder()
            .id("test-create-cluster")
            .type(CreateCluster.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .clusterName(Property.ofValue("my-test-cluster"))
            .kafkaVersion(Property.ofValue("3.5.1"))
            .numberOfBrokerNodes(Property.ofValue(3))
            .instanceType(Property.ofValue("kafka.m5.large"))
            .clientSubnets(Property.ofValue(List.of("subnet-aaa", "subnet-bbb", "subnet-ccc")))
            .securityGroups(Property.ofValue(List.of("sg-111")))
            .build();

        var mockResponse = CreateClusterResponse.builder()
            .clusterArn("arn:aws:kafka:us-east-1:123456789012:cluster/my-test-cluster/abc123")
            .clusterName("my-test-cluster")
            .state(ClusterState.CREATING)
            .build();

        var mockClient = mock(KafkaClient.class);
        when(mockClient.createCluster(any(CreateClusterRequest.class))).thenReturn(mockResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getClusterArn(), containsString("my-test-cluster"));
        assertThat(output.getClusterName(), is("my-test-cluster"));
        assertThat(output.getState(), is("CREATING"));
    }
}

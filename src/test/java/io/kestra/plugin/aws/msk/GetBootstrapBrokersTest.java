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
class GetBootstrapBrokersTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenClusterArn_whenGetBrokers_thenOutputContainsBrokerStrings() throws Exception {
        var runContext = runContextFactory.of();

        var task = GetBootstrapBrokers.builder()
            .id("test-get-brokers")
            .type(GetBootstrapBrokers.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .clusterArn(Property.ofValue("arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc123"))
            .build();

        var mockResponse = GetBootstrapBrokersResponse.builder()
            .bootstrapBrokerString("b-1.my-cluster.abc123.c1.kafka.us-east-1.amazonaws.com:9092,b-2.my-cluster.abc123.c1.kafka.us-east-1.amazonaws.com:9092")
            .bootstrapBrokerStringTls("b-1.my-cluster.abc123.c1.kafka.us-east-1.amazonaws.com:9094,b-2.my-cluster.abc123.c1.kafka.us-east-1.amazonaws.com:9094")
            .build();

        var mockClient = mock(KafkaClient.class);
        when(mockClient.getBootstrapBrokers(any(GetBootstrapBrokersRequest.class))).thenReturn(mockResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getBootstrapBrokerString(), containsString("9092"));
        assertThat(output.getBootstrapBrokerStringTls(), containsString("9094"));
    }
}

package io.kestra.plugin.aws;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sts.StsClient;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConnectionUtilsTest {
    @Test
    void configureClient_shouldIgnoreBlankEndpointOverride() {
        AbstractConnection.AwsClientConfig config = mock(AbstractConnection.AwsClientConfig.class);

        when(config.region()).thenReturn("us-east-1");
        when(config.endpointOverride()).thenReturn("");

        assertDoesNotThrow(() -> {
            ConnectionUtils.configureClient(config, StsClient.builder());
        });
    }
}
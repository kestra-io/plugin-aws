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

        assertDoesNotThrow(() ->
        {
            ConnectionUtils.configureClient(config, StsClient.builder());
        });
    }

    @Test
    void stsClient_shouldNotThrowWhenRegionIsNull() {
        AbstractConnection.AwsClientConfig config = mock(AbstractConnection.AwsClientConfig.class);

        when(config.region()).thenReturn(null);
        when(config.stsEndpointOverride()).thenReturn(null);
        when(config.stsRoleArn()).thenReturn("arn:aws:iam::123456789012:role/TestRole");
        when(config.stsRoleSessionName()).thenReturn("test-session");
        when(config.stsRoleExternalId()).thenReturn(null);
        when(config.stsRoleSessionDuration()).thenReturn(java.time.Duration.ofSeconds(900));

        // Should not throw DefaultAwsRegionProviderChain exception when region is not set
        assertDoesNotThrow(() -> ConnectionUtils.stsClient(config));
    }
}
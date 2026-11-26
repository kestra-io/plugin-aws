package io.kestra.plugin.aws.kinesis;

import java.util.List;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class ConsumeUnitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void runClosesKinesisClient() throws Exception {
        var runContext = runContextFactory.of();
        var client = mock(KinesisClient.class);
        when(client.listShards(any(ListShardsRequest.class)))
            .thenReturn(ListShardsResponse.builder().shards(List.of()).build());

        var task = spy(
            Consume.builder()
                .id(IdUtils.create())
                .type(Consume.class.getName())
                .region(Property.ofValue("us-east-1"))
                .accessKeyId(Property.ofValue("test"))
                .secretKeyId(Property.ofValue("test"))
                .streamName(Property.ofValue("stream"))
                .build()
        );
        doReturn(client).when(task).client(any());

        task.run(runContext);

        verify(client, times(1)).close();
    }
}

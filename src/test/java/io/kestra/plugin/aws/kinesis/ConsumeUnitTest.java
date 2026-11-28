package io.kestra.plugin.aws.kinesis;

import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.kinesis.model.Record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class ConsumeUnitTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void consumesShardsAcrossPages() throws Exception {
        var client = mock(KinesisClient.class);
        var firstRequest = ListShardsRequest.builder().streamName("stream").build();
        var nextRequest = ListShardsRequest.builder().nextToken("next-page").build();
        when(client.listShards(firstRequest)).thenReturn(
            ListShardsResponse.builder().shards(Shard.builder().shardId("shard-a").build()).nextToken("next-page").build()
        );
        when(client.listShards(nextRequest)).thenReturn(
            ListShardsResponse.builder().shards(Shard.builder().shardId("shard-b").build()).build()
        );
        when(client.getShardIterator(any(GetShardIteratorRequest.class))).thenAnswer(invocation ->
        {
            GetShardIteratorRequest request = invocation.getArgument(0);
            return GetShardIteratorResponse.builder().shardIterator(request.shardId()).build();
        });
        when(client.getRecords(any(GetRecordsRequest.class))).thenAnswer(invocation ->
        {
            GetRecordsRequest request = invocation.getArgument(0);
            var sequence = request.shardIterator().equals("shard-a") ? "1" : "2";
            return GetRecordsResponse.builder().records(
                Record.builder()
                    .data(SdkBytes.fromUtf8String("record"))
                    .partitionKey("key")
                    .sequenceNumber(sequence)
                    .approximateArrivalTimestamp(Instant.EPOCH)
                    .build()
            ).build();
        });

        var consume = spy(
            Consume.builder()
                .streamName(Property.ofValue("stream"))
                .iteratorType(Property.ofValue(AbstractKinesis.IteratorType.TRIM_HORIZON))
                .maxRecords(Property.ofValue(2))
                .build()
        );
        doReturn(client).when(consume).client(any());

        var output = consume.run(runContextFactory.of());

        assertEquals(2, output.getCount());
        assertEquals(Map.of("shard-a", "1", "shard-b", "2"), output.getLastSequencePerShard());
        verify(client).listShards(nextRequest);
    }
}

package io.kestra.plugin.aws.kinesis;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.aws.kinesis.model.Record;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.services.kinesis.model.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ConsumeTest extends AbstractKinesisTest {
    private static List<Consume.ConsumedRecord> loadOutput(RunContext ctx, URI uri) throws Exception {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(ctx.storage().getFile(uri)))) {
            return FileSerde.readAll(r, Consume.ConsumedRecord.class).collectList().block();
        }
    }

    @Test
    void testConsume() throws Exception {
        var runContext = runContextFactory.of();

        Record record = Record.builder()
            .partitionKey("pk")
            .data("Hello")
            .build();

        var put = PutRecords.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .streamName(Property.ofValue(streamName))
            .records(List.of(record))
            .build();

        put.run(runContext);

        var consume = Consume.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .streamName(Property.ofValue(streamName))
            .iteratorType(Property.ofValue(AbstractKinesis.IteratorType.TRIM_HORIZON))
            .maxRecords(Property.ofValue(1))
            .pollDuration(Property.ofValue(java.time.Duration.ofSeconds(1)))
            .build();

        var output = consume.run(runContext);

        assertThat(output.getCount(), is(1));

        List<Consume.ConsumedRecord> records = loadOutput(runContext, output.getUri());
        assertThat(records, hasSize(1));

        assertThat(records.getFirst().getData(), startsWith("Hello"));
        assertThat(records.getFirst().getPartitionKey(), equalTo("pk"));
        assertThat(records.getFirst().getSequenceNumber(), notNullValue());
        assertThat(records.getFirst().getShardId(), notNullValue());
        assertThat(records.getFirst().getApproximateArrivalTimestamp(), instanceOf(Instant.class));
    }
}

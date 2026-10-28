package io.kestra.plugin.aws.kinesis;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class PutRecordsBatchingTest {
    private static final int TWO_AND_A_HALF_MIB = 5 * 1024 * 1024 / 2;

    private static PutRecordsRequestEntry entry(String partitionKey, int dataBytes) {
        return PutRecordsRequestEntry.builder()
            .partitionKey(partitionKey)
            .data(SdkBytes.fromByteArray(new byte[dataBytes]))
            .build();
    }

    private static List<PutRecordsRequestEntry> smallEntries(int count) {
        return IntStream.range(0, count).mapToObj(i -> entry("key-" + i, 10)).toList();
    }

    private static List<Integer> batchSizes(List<List<PutRecordsRequestEntry>> batches) {
        return batches.stream().map(List::size).toList();
    }

    @Test
    void partitionEmpty() {
        assertThat(PutRecords.partition(List.of()), is(empty()));
    }

    @Test
    void partitionBelowLimitIsASingleBatch() {
        assertThat(batchSizes(PutRecords.partition(smallEntries(3))), contains(3));
    }

    @Test
    void partitionAtRecordLimitIsASingleBatch() {
        assertThat(batchSizes(PutRecords.partition(smallEntries(500))), contains(500));
    }

    @Test
    void partitionAboveRecordLimitSplitsInTwo() {
        assertThat(batchSizes(PutRecords.partition(smallEntries(501))), contains(500, 1));
    }

    @Test
    void partitionKeepsInputOrder() {
        var entries = smallEntries(1_201);

        var batches = PutRecords.partition(entries);

        assertThat(batchSizes(batches), contains(500, 500, 201));
        assertThat(batches.stream().flatMap(List::stream).toList(), equalTo(entries));
    }

    @Test
    void partitionAtByteLimitIsASingleBatch() {
        // Each entry is exactly 2.5 MiB with its 1 byte partition key, so the two together are exactly 5 MiB.
        var entries = List.of(entry("a", TWO_AND_A_HALF_MIB - 1), entry("b", TWO_AND_A_HALF_MIB - 1));

        assertThat(batchSizes(PutRecords.partition(entries)), contains(2));
    }

    @Test
    void partitionAboveByteLimitSplitsBeforeTheEntryThatOverflows() {
        // Five entries of 1 MiB data plus a partition key do not fit in 5 MiB together.
        var entries = IntStream.range(0, 5).mapToObj(i -> entry("k", 1024 * 1024)).toList();

        assertThat(batchSizes(PutRecords.partition(entries)), contains(4, 1));
    }

    @Test
    void partitionCountsThePartitionKeyInTheByteLimit() {
        // The data alone is exactly 5 MiB in total, the partition keys make it overflow.
        var entries = List.of(entry("a", TWO_AND_A_HALF_MIB), entry("b", TWO_AND_A_HALF_MIB));

        assertThat(batchSizes(PutRecords.partition(entries)), contains(1, 1));
    }

    @Test
    void partitionKeepsAnOversizedEntryInItsOwnBatch() {
        var entries = List.of(entry("a", 10), entry("huge", 6 * 1024 * 1024), entry("b", 10));

        assertThat(batchSizes(PutRecords.partition(entries)), contains(1, 1, 1));
    }

    @Test
    void putInBatchesAggregatesFailuresAcrossBatchesAndKeepsOrder() {
        var entries = smallEntries(1_201);
        var sentBatchSizes = new ArrayList<Integer>();

        // The second batch has 2 rejected records; the first and last succeed.
        PutRecords.BatchResult result = PutRecords.putInBatches(entries, batch -> {
            int batchIndex = sentBatchSizes.size();
            sentBatchSizes.add(batch.size());
            var resultEntries = new ArrayList<PutRecordsResultEntry>();
            for (int i = 0; i < batch.size(); i++) {
                boolean failed = batchIndex == 1 && i < 2;
                resultEntries.add(
                    PutRecordsResultEntry.builder()
                        .sequenceNumber(failed ? null : batch.get(i).partitionKey())
                        .errorCode(failed ? "ProvisionedThroughputExceededException" : null)
                        .build()
                );
            }
            return PutRecordsResponse.builder()
                .records(resultEntries)
                .failedRecordCount(batchIndex == 1 ? 2 : 0)
                .build();
        });

        // Every batch got an attempt, even after the one with failures.
        assertThat(sentBatchSizes, contains(500, 500, 201));
        assertThat(result.failedRecordCount(), is(2));
        assertThat(result.entries(), hasSize(1_201));
        // Results line up with the input entries: failed ones are at positions 500 and 501.
        assertThat(result.entries().get(499).sequenceNumber(), is("key-499"));
        assertThat(result.entries().get(500).errorCode(), is("ProvisionedThroughputExceededException"));
        assertThat(result.entries().get(501).errorCode(), is("ProvisionedThroughputExceededException"));
        assertThat(result.entries().get(502).sequenceNumber(), is("key-502"));
        assertThat(result.entries().get(1_200).sequenceNumber(), is("key-1200"));
    }

    @Test
    void putInBatchesWithoutEntriesSendsNothing() {
        PutRecords.BatchResult result = PutRecords.putInBatches(List.of(), batch -> {
            throw new AssertionError("no request must be sent");
        });

        assertThat(result.entries(), is(empty()));
        assertThat(result.failedRecordCount(), is(0));
    }
}

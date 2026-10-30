package io.kestra.plugin.aws.kinesis;

import java.time.Duration;
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

class RecordBatcherTest {
    private static final int TWO_AND_A_HALF_MIB = 5 * 1024 * 1024 / 2;
    private static final String THROTTLED = "ProvisionedThroughputExceededException";

    private static PutRecordsRequestEntry entry(String partitionKey, int dataBytes) {
        return PutRecordsRequestEntry.builder()
            .partitionKey(partitionKey)
            .data(SdkBytes.fromByteArray(new byte[dataBytes]))
            .build();
    }

    private static PutRecordsResultEntry success(PutRecordsRequestEntry entry) {
        return PutRecordsResultEntry.builder().sequenceNumber(entry.partitionKey()).shardId("shardId-000000000000").build();
    }

    private static PutRecordsResultEntry failure(String errorCode) {
        return PutRecordsResultEntry.builder().errorCode(errorCode).errorMessage(errorCode + " message").build();
    }

    private static PutRecordsResponse response(List<PutRecordsResultEntry> results) {
        var failed = (int) results.stream().filter(r -> r.errorCode() != null).count();
        return PutRecordsResponse.builder().records(results).failedRecordCount(failed).build();
    }

    // Records every request, sleep and result; the sender succeeds unless a test gives its own.
    private static final class Recorder {
        final List<List<PutRecordsRequestEntry>> requests = new ArrayList<>();
        final List<Duration> sleeps = new ArrayList<>();
        final List<String> items = new ArrayList<>();
        final List<PutRecordsResultEntry> results = new ArrayList<>();

        RecordBatcher<String> batcher(RecordBatcher.Sender sender) {
            return new RecordBatcher<>(
                entries -> {
                    requests.add(List.copyOf(entries));
                    return sender.send(entries);
                },
                sleeps::add,
                (item, result) -> {
                    items.add(item);
                    results.add(result);
                }
            );
        }

        RecordBatcher<String> batcher() {
            return batcher(entries -> response(entries.stream().map(RecordBatcherTest::success).toList()));
        }

        List<Integer> requestSizes() {
            return requests.stream().map(List::size).toList();
        }
    }

    private static void addAll(RecordBatcher<String> batcher, List<PutRecordsRequestEntry> entries) throws Exception {
        for (var entry : entries) {
            batcher.add(entry.partitionKey(), entry);
        }
        batcher.flush();
    }

    private static List<PutRecordsRequestEntry> smallEntries(int count) {
        return IntStream.range(0, count).mapToObj(i -> entry("key-" + i, 10)).toList();
    }

    @Test
    void emptyInputSendsNothing() throws Exception {
        var recorder = new Recorder();

        addAll(recorder.batcher(), List.of());

        assertThat(recorder.requests, is(empty()));
        assertThat(recorder.results, is(empty()));
    }

    @Test
    void recordLimitBoundaries() throws Exception {
        for (var count : List.of(3, 500, 501, 1_201)) {
            var recorder = new Recorder();
            addAll(recorder.batcher(), smallEntries(count));

            var expected = switch (count) {
                case 3 -> List.of(3);
                case 500 -> List.of(500);
                case 501 -> List.of(500, 1);
                default -> List.of(500, 500, 201);
            };
            assertThat("count " + count, recorder.requestSizes(), equalTo(expected));
        }
    }

    @Test
    void resultsComeBackInInputOrder() throws Exception {
        var recorder = new Recorder();
        var entries = smallEntries(1_201);

        addAll(recorder.batcher(), entries);

        assertThat(recorder.items, equalTo(entries.stream().map(PutRecordsRequestEntry::partitionKey).toList()));
        assertThat(recorder.results.stream().map(PutRecordsResultEntry::sequenceNumber).toList(), equalTo(recorder.items));
    }

    @Test
    void exactlyFiveMibStaysInOneRequest() throws Exception {
        var recorder = new Recorder();

        // Each entry is 2.5 MiB with its 1 byte partition key, so both together are exactly 5 MiB.
        addAll(recorder.batcher(), List.of(entry("a", TWO_AND_A_HALF_MIB - 1), entry("b", TWO_AND_A_HALF_MIB - 1)));

        assertThat(recorder.requestSizes(), contains(2));
    }

    @Test
    void partitionKeyCountsTowardsTheByteLimit() throws Exception {
        var recorder = new Recorder();

        addAll(recorder.batcher(), List.of(entry("a", TWO_AND_A_HALF_MIB), entry("b", TWO_AND_A_HALF_MIB)));

        assertThat(recorder.requestSizes(), contains(1, 1));
    }

    @Test
    void overflowingEntryStartsANewRequest() throws Exception {
        var recorder = new Recorder();

        addAll(recorder.batcher(), IntStream.range(0, 5).mapToObj(i -> entry("k" + i, 1024 * 1024)).toList());

        assertThat(recorder.requestSizes(), contains(4, 1));
    }

    @Test
    void oversizedEntryIsSentAlone() throws Exception {
        var recorder = new Recorder();

        addAll(recorder.batcher(), List.of(entry("a", 10), entry("huge", 6 * 1024 * 1024), entry("b", 10)));

        assertThat(recorder.requestSizes(), contains(1, 1, 1));
    }

    @Test
    void throttledRecordsAreRetriedAndReplacedInPlace() throws Exception {
        var recorder = new Recorder();
        var calls = new int[] { 0 };
        // The first request throttles the records at positions 1 and 3; the retry accepts them.
        var batcher = recorder.batcher(entries -> {
            var first = calls[0]++ == 0;
            return response(IntStream.range(0, entries.size())
                .mapToObj(i -> first && (i == 1 || i == 3) ? failure(THROTTLED) : success(entries.get(i)))
                .toList());
        });

        addAll(batcher, smallEntries(5));

        assertThat(recorder.requestSizes(), contains(5, 2));
        assertThat(recorder.requests.get(1).stream().map(PutRecordsRequestEntry::partitionKey).toList(), contains("key-1", "key-3"));
        assertThat(recorder.sleeps, contains(Duration.ofMillis(100)));
        assertThat(recorder.results.stream().map(PutRecordsResultEntry::sequenceNumber).toList(), contains("key-0", "key-1", "key-2", "key-3", "key-4"));
        assertThat(recorder.results.stream().filter(r -> r.errorCode() != null).count(), is(0L));
    }

    @Test
    void retriesStopAfterTheBoundWithBackoff() throws Exception {
        var recorder = new Recorder();
        var batcher = recorder.batcher(entries -> response(entries.stream().map(e -> failure("InternalFailure")).toList()));

        addAll(batcher, smallEntries(2));

        assertThat(recorder.requestSizes(), contains(2, 2, 2, 2));
        assertThat(recorder.sleeps, contains(Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(400)));
        assertThat(recorder.results.stream().map(PutRecordsResultEntry::errorCode).toList(), contains("InternalFailure", "InternalFailure"));
    }

    @Test
    void nonRetryableFailuresAreNotRetried() throws Exception {
        var recorder = new Recorder();
        var batcher = recorder.batcher(entries -> response(entries.stream().map(e -> failure("KMSAccessDeniedException")).toList()));

        addAll(batcher, smallEntries(2));

        assertThat(recorder.requestSizes(), contains(2));
        assertThat(recorder.sleeps, is(empty()));
        assertThat(recorder.results.stream().map(PutRecordsResultEntry::errorCode).toList(), contains("KMSAccessDeniedException", "KMSAccessDeniedException"));
    }

    @Test
    void failureMessageKeepsOnlyTheFirstFailures() {
        var summary = new PutRecords.ResultSummary();
        for (var i = 0; i < 1_201; i++) {
            summary.add(i % 50 == 7 ? failure(THROTTLED) : PutRecordsResultEntry.builder().sequenceNumber(String.valueOf(i)).build());
        }

        var message = summary.failureMessage();

        assertThat(summary.getCount(), is(1_201));
        assertThat(summary.getFailedCount(), is(24));
        assertThat(summary.getFirstFailures(), hasSize(PutRecords.MAX_FAILURES_IN_MESSAGE));
        assertThat(message, startsWith("24 of 1201 records failed (throttled records were retried up to 3 times). First 10 failures: position 7: " + THROTTLED));
        assertThat(message, not(containsString("position 507:")));
        assertThat(message.length(), lessThan(2_000));
    }
}

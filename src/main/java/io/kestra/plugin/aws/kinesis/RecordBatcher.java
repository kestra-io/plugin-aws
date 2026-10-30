package io.kestra.plugin.aws.kinesis;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

// Sends records in PutRecords requests within the Kinesis limits, holding one request at a time and retrying throttled records.
final class RecordBatcher<T> {
    static final int MAX_RECORDS_PER_REQUEST = 500;
    static final int MAX_BYTES_PER_REQUEST = 5 * 1024 * 1024;
    static final int MAX_RETRIES = 3;
    static final Duration BASE_BACKOFF = Duration.ofMillis(100);

    // Per-record errors that Kinesis documents as transient; other codes (e.g. KMS errors) are not retried.
    private static final Set<String> RETRYABLE_ERROR_CODES = Set.of("ProvisionedThroughputExceededException", "InternalFailure");

    @FunctionalInterface
    interface Sender {
        PutRecordsResponse send(List<PutRecordsRequestEntry> entries);
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    @FunctionalInterface
    interface ResultHandler<T> {
        void accept(T item, PutRecordsResultEntry result) throws Exception;
    }

    private final Sender sender;
    private final Sleeper sleeper;
    private final ResultHandler<T> resultHandler;
    private final List<T> items = new ArrayList<>();
    private final List<PutRecordsRequestEntry> entries = new ArrayList<>();
    private long bytes = 0L;

    RecordBatcher(Sender sender, Sleeper sleeper, ResultHandler<T> resultHandler) {
        this.sender = sender;
        this.sleeper = sleeper;
        this.resultHandler = resultHandler;
    }

    void add(T item, PutRecordsRequestEntry entry) throws Exception {
        var entryBytes = sizeOf(entry);
        if (!entries.isEmpty() && (entries.size() >= MAX_RECORDS_PER_REQUEST || bytes + entryBytes > MAX_BYTES_PER_REQUEST)) {
            flush();
        }
        items.add(item);
        entries.add(entry);
        bytes += entryBytes;
    }

    void flush() throws Exception {
        if (entries.isEmpty()) {
            return;
        }

        var results = sendWithRetry(entries);
        for (var i = 0; i < items.size(); i++) {
            resultHandler.accept(items.get(i), results.get(i));
        }

        items.clear();
        entries.clear();
        bytes = 0L;
    }

    // Resends only the retryable failed entries and puts their new result in place, so results stay aligned with the input.
    private List<PutRecordsResultEntry> sendWithRetry(List<PutRecordsRequestEntry> batch) throws InterruptedException {
        var results = new ArrayList<>(sender.send(batch).records());

        for (var attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            var retryIndexes = IntStream.range(0, results.size())
                .filter(i -> isRetryable(results.get(i)))
                .boxed()
                .toList();
            if (retryIndexes.isEmpty()) {
                break;
            }

            sleeper.sleep(backoff(attempt));
            var retried = sender.send(retryIndexes.stream().map(batch::get).toList()).records();
            for (var i = 0; i < retryIndexes.size(); i++) {
                results.set(retryIndexes.get(i), retried.get(i));
            }
        }

        return results;
    }

    static Duration backoff(int attempt) {
        return BASE_BACKOFF.multipliedBy(1L << (attempt - 1));
    }

    private static boolean isRetryable(PutRecordsResultEntry result) {
        return result.errorCode() != null && RETRYABLE_ERROR_CODES.contains(result.errorCode());
    }

    // Kinesis counts the data and the partition key towards the 5 MiB limit.
    private static long sizeOf(PutRecordsRequestEntry entry) {
        return entry.data().asByteArrayUnsafe().length + (long) entry.partitionKey().getBytes(StandardCharsets.UTF_8).length;
    }
}

package io.kestra.plugin.aws.billing;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

/**
 * Applies a {@link FetchType} to rows already collected from a billing API, the same way athena.Query does:
 * STORE writes an ION file, FETCH returns all rows, FETCH_ONE the first one, NONE only the count.
 */
final class BillingFetch {
    private BillingFetch() {
    }

    record Result(List<Map<String, Object>> rows, Map<String, Object> row, URI uri, long size) {
    }

    static Result apply(RunContext runContext, FetchType fetchType, List<Map<String, Object>> items) throws IOException {
        Result result = switch (fetchType) {
            case FETCH -> new Result(items, null, null, items.size());
            case FETCH_ONE -> {
                var first = items.isEmpty() ? null : items.getFirst();
                yield new Result(null, first, null, first == null ? 0 : 1);
            }
            case STORE -> new Result(null, null, store(runContext, items), items.size());
            case NONE -> new Result(null, null, null, items.size());
        };
        runContext.metric(Counter.of("fetch.rows", result.size()));
        return result;
    }

    static int maxPages(RunContext runContext, Property<Integer> maxPages) throws IllegalVariableEvaluationException {
        int value = runContext.render(maxPages).as(Integer.class).orElseThrow();
        if (value < 1) {
            throw new IllegalArgumentException("maxPages must be at least 1, got " + value);
        }
        return value;
    }

    private static URI store(RunContext runContext, List<Map<String, Object>> items) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var out = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            for (var item : items) {
                FileSerde.write(out, item);
            }
        }
        return runContext.storage().putFile(tempFile);
    }
}

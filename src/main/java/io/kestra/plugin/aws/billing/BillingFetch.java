package io.kestra.plugin.aws.billing;

import java.io.BufferedOutputStream;
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

final class BillingFetch {
    private BillingFetch() {
    }

    record Result(List<Map<String, Object>> rows, Map<String, Object> row, URI uri, long size) {
    }

    static Result apply(RunContext runContext, FetchType fetchType, List<Map<String, Object>> items) throws IOException {
        var result = switch (fetchType) {
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

    static final int DEFAULT_MAX_PAGES = 10;
    static final int MAX_MAX_PAGES = 1000;

    static String requiredString(RunContext runContext, Property<String> property, String name) throws IllegalVariableEvaluationException {
        var rValue = runContext.render(property).as(String.class).orElse(null);
        if (rValue == null || rValue.isBlank()) {
            throw new IllegalArgumentException(name + " is required but rendered empty, check the expression that sets it");
        }
        return rValue;
    }

    static int maxPages(RunContext runContext, Property<Integer> maxPages) throws IllegalVariableEvaluationException {
        var rMaxPages = runContext.render(maxPages).as(Integer.class).orElse(DEFAULT_MAX_PAGES);
        if (rMaxPages < 1 || rMaxPages > MAX_MAX_PAGES) {
            throw new IllegalArgumentException("maxPages must be between 1 and " + MAX_MAX_PAGES + ", got " + rMaxPages);
        }
        return rMaxPages;
    }

    private static URI store(RunContext runContext, List<Map<String, Object>> items) throws IOException {
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var out = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            for (var item : items) {
                FileSerde.write(out, item);
            }
        }
        return runContext.storage().putFile(tempFile);
    }
}

package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.models.property.Property;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class TriggerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void evaluate() throws Exception {
        var trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(TriggerTest.class.getSimpleName())
            .namespace(Property.ofValue("Custom/Test"))
            .metricName(Property.ofValue("TriggerLatency"))
            .statistic(Property.ofValue("Average"))
            .periodSeconds(Property.ofValue(60))
            .window(Property.ofValue(Duration.ofMinutes(5)))
            .dimensions(Property.ofValue(List.of(
                Query.DimensionKV.builder()
                    .name(Property.ofValue("env"))
                    .value(Property.ofValue("test"))
                    .build()
            )))
            .build();

        var output = Query.Output.builder()
            .count(1)
            .series(List.of(Map.of("average", 456.7)))
            .build();

        try (MockedConstruction<Query> mockedQuery = mockConstruction(Query.class, (mock, context) ->
            when(mock.run(any())).thenReturn(output)
        )) {
            var conditionContext = io.kestra.core.utils.TestsUtils.mockTrigger(runContextFactory, trigger);
            var execution = trigger.evaluate(conditionContext.getKey(), conditionContext.getValue());

            assertThat(execution.isPresent(), is(true));
            assertThat(mockedQuery.constructed(), hasSize(1));
            verify(mockedQuery.constructed().getFirst()).run(any());
        }
    }
}

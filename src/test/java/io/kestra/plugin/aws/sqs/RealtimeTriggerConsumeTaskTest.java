package io.kestra.plugin.aws.sqs;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class RealtimeTriggerConsumeTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void consumeTaskCarriesTheTriggerClientOptions() throws Exception {
        var trigger = RealtimeTrigger.builder()
            .id("sqs-realtime")
            .type(RealtimeTrigger.class.getName())
            .queueUrl(Property.ofValue("http://localhost:4566/000000000000/queue"))
            .maxConcurrency(Property.ofValue(7))
            .connectionAcquisitionTimeout(Property.ofValue(Duration.ofSeconds(42)))
            .build();
        var runContext = runContextFactory.of();

        var task = trigger.consumeTask();

        assertThat(runContext.render(task.getMaxConcurrency()).as(Integer.class).orElseThrow(), is(7));
        assertThat(runContext.render(task.getConnectionAcquisitionTimeout()).as(Duration.class).orElseThrow(), is(Duration.ofSeconds(42)));
    }
}

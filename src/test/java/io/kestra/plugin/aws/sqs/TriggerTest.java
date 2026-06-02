package io.kestra.plugin.aws.sqs;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.junit.annotations.LoadFlows;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Scheduler;
import io.kestra.plugin.aws.sqs.model.Message;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest(startRunner = true, startScheduler = true)
class TriggerTest extends AbstractSqsTest {
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected Scheduler scheduler;

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    @LoadFlows({"flows/sqs/sqs-listen.yaml"})
    void flow() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).until(() -> scheduler.isActive());

        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> lastExecution = new AtomicReference<>();
        executionQueue.addListener(execution -> {
            lastExecution.set(execution);
            queueCount.countDown();
            assertThat(execution.getFlowId(), is("sqs-listen"));
        });

        Publish task = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(
                List.of(
                    Message.builder().data("Hello World").build(),
                    Message.builder().data("Hello Kestra").delaySeconds(5).build()
                )
            )
            .build();

        var runContext = runContextFactory.of();

        task.run(runContext);

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        Execution last = lastExecution.get();
        var count = (Integer) last.getTrigger().getVariables().get("count");
        var uri = (String) last.getTrigger().getVariables().get("uri");
        assertThat(count, is(2));
        assertThat(uri, is(notNullValue()));
    }

}

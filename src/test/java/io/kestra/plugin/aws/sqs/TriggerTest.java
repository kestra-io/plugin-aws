package io.kestra.plugin.aws.sqs;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.aws.sqs.model.Message;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.worker.DefaultWorker;

import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class TriggerTest extends AbstractSqsTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void flow() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, UUID.randomUUID().toString(), 8, null);
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            )
        ) {
            // wait for execution
            AtomicReference<Execution> lastExecution = new AtomicReference<>();
            executionQueue.addListener(execution -> {
                lastExecution.set(execution);
                queueCount.countDown();
                assertThat(execution.getFlowId(), is("sqs-listen"));
            });

            worker.run();
            scheduler.run();

            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/sqs/sqs-listen.yaml")));

            // publish two messages to trigger the flow
            Publish task = Publish.builder()
                .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString()))
                .queueUrl(Property.ofValue(queueUrl()))
                .region(Property.ofValue(localstack.getRegion()))
                .accessKeyId(Property.ofValue(localstack.getAccessKey()))
                .secretKeyId(Property.ofValue(localstack.getSecretKey()))
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

}

package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.plugin.aws.sqs.model.Message;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@Disabled("Issue with LocalStack, see https://github.com/localstack/localstack/issues/8267")
class TriggerTest extends AbstractSqsTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private SchedulerTriggerStateInterface triggerState;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    private RunContextFactory runContextFactory;


    @Test
    void flow() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        Worker worker = new Worker(applicationContext, 8, null);
        try (
            AbstractScheduler scheduler = new DefaultScheduler(
                this.applicationContext,
                this.flowListenersService,
                this.triggerState
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(TriggerTest.class, execution -> {
                last.set(execution.getLeft());

                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("sqs-listen"));
            });

            worker.run();
            scheduler.run();

            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/sqs")));

            // publish two messages to trigger the flow
            Publish task = Publish.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString())
                .queueUrl(queueUrl())
                .region(localstack.getRegion())
                .accessKeyId(localstack.getAccessKey())
                .secretKeyId(localstack.getSecretKey())
                .from(
                    List.of(
                        Message.builder().data("Hello World").build(),
                        Message.builder().data("Hello Kestra").delaySeconds(5).build()
                    )
                )
                .build();

            var runContext = runContextFactory.of();
            var client = task.client(runContext);
            createQueue(client);

            task.run(runContext);

            queueCount.await(1, TimeUnit.MINUTES);

            @SuppressWarnings("unchecked")
            var count = (Integer) last.get().getTrigger().getVariables().get("count");
            var uri = (String) last.get().getTrigger().getVariables().get("uri");
            assertThat(count, is(2));
            assertThat(uri, is(notNullValue()));
        }
    }

}

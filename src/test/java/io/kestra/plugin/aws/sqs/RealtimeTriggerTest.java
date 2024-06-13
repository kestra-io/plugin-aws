package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.aws.sqs.model.Message;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Disabled("Issue with LocalStack, see https://github.com/localstack/localstack/issues/8267")
class RealtimeTriggerTest extends AbstractSqsTest {
    @Inject
    private ApplicationContext applicationContext;

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
        Worker worker = applicationContext.createBean(Worker.class, UUID.randomUUID().toString(), 8, null);
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            )
        ) {
            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("realtime"));
            });

            worker.run();
            scheduler.run();

            repositoryLoader.load(Objects.requireNonNull(RealtimeTriggerTest.class.getClassLoader().getResource("flows/sqs/realtime.yaml")));

            // publish two messages to trigger the flow
            Publish task = Publish.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString())
                .queueUrl(queueUrl())
                .region(localstack.getRegion())
                .accessKeyId(localstack.getAccessKey())
                .secretKeyId(localstack.getSecretKey())
                .from(
                    List.of(
                        Message.builder().data("Hello World").build()
                    )
                )
                .build();

            var runContext = runContextFactory.of();
            var client = task.client(runContext);
            createQueue(client);

            task.run(runContext);

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));

            Execution last = receive.blockLast();
            assertThat(last.getTrigger().getVariables().size(), is(1));
            assertThat(last.getTrigger().getVariables().get("data"), is("Hello World"));
        }
    }

}

package io.kestra.plugin.aws.s3;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.plugin.aws.s3.models.S3Object;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class TriggerTest extends AbstractTest {
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

    @Test
    void deleteAction() throws Exception {
        String bucket = "trigger-test";
        this.createBucket(bucket);
        List listTask = list().bucket(bucket).build();

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
            executionQueue.receive(TriggerTest.class, executionWithError -> {
                Execution execution = executionWithError.getLeft();

                if (execution.getFlowId().equals("s3-listen")) {
                    last.set(execution);
                    queueCount.countDown();
                }
            });


            upload("trigger/s3", bucket);
            upload("trigger/s3", bucket);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/s3/s3-listen.yaml")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            try {
                assertThat(await, is(true));
            } finally {
                worker.shutdown();
            }

            @SuppressWarnings("unchecked")
            java.util.List<S3Object> trigger = (java.util.List<S3Object>) last.get().getTrigger().getVariables().get("objects");

            assertThat(trigger.size(), is(2));

            int remainingFilesOnBucket = listTask.run(runContext(listTask))
                .getObjects()
                .size();
            assertThat(remainingFilesOnBucket, is(0));
        }
    }

    @Test
    void noneAction() throws Exception {
        String bucket = "trigger-none-action-test";
        this.createBucket(bucket);
        List listTask = list().bucket(bucket).build();

        // wait for execution
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();
        executionQueue.receive(TriggerTest.class, executionWithError -> {
            Execution execution = executionWithError.getLeft();

            if (execution.getFlowId().equals("s3-listen-none-action")) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        // scheduler
        Worker worker = new Worker(applicationContext, 8, null);
        try (
            AbstractScheduler scheduler = new DefaultScheduler(
                this.applicationContext,
                this.flowListenersService,
                this.triggerState
            );
        ) {
            upload("trigger/s3", bucket);
            upload("trigger/s3", bucket);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/s3/s3-listen-none-action.yaml")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            try {
                assertThat(await, is(true));
            } finally {
                worker.shutdown();
            }

            @SuppressWarnings("unchecked")
            java.util.List<S3Object> trigger = (java.util.List<S3Object>) last.get().getTrigger().getVariables().get("objects");

            assertThat(trigger.size(), is(2));

            int remainingFilesOnBucket = listTask.run(runContext(listTask))
                .getObjects()
                .size();
            assertThat(remainingFilesOnBucket, is(2));
        }
    }
}

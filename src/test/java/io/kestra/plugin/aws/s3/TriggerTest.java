package io.kestra.plugin.aws.s3;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.core.utils.IdUtils;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.aws.s3.models.S3Object;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class TriggerTest extends AbstractTest {
    @Inject
    private ApplicationContext applicationContext;

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
        List listTask = list().bucket(Property.ofValue(bucket)).build();

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
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError -> {
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
                receive.blockLast();
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
        List listTask = list().bucket(Property.ofValue(bucket)).build();

        // wait for execution
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();
        Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError -> {
            Execution execution = executionWithError.getLeft();

            if (execution.getFlowId().equals("s3-listen-none-action")) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        // scheduler
        DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, UUID.randomUUID().toString(), 8, null);
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            )
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
                receive.blockLast();
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

    @Test
    void forcePathStyleWithSimpleLocalhost() throws Exception {
        String bucket = "trigger-force-path-style-test";
        this.createBucket(bucket);
        List listTask = list().bucket(Property.ofValue(bucket)).build();

        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, UUID.randomUUID().toString(), 8, null);
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            )
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError -> {
                Execution execution = executionWithError.getLeft();

                if (execution.getFlowId().equals("s3-listen-localhost-force-path-style")) {
                    last.set(execution);
                    queueCount.countDown();
                }
            });

            upload("trigger/s3", bucket);
            upload("trigger/s3", bucket);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/s3/s3-listen-localhost-force-path-style.yaml")));

            boolean await = queueCount.await(15, TimeUnit.SECONDS);
            try {
                assertThat("trigger should work with localhost endpoint + forcePathStyle", await, is(true));
            } finally {
                worker.shutdown();
                receive.blockLast();
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
    void shouldExecuteOnCreate() throws Exception {
        String bucket = "trigger-on-create";
        this.createBucket(bucket);

        Trigger trigger = Trigger.builder()
            .id("s3-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue("trigger/on-create"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .interval(Duration.ofSeconds(10))
            .build();

        upload("trigger/on-create", bucket);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));
    }

    @Test
    void shouldExecuteOnUpdate() throws Exception {
        String bucket = "trigger-on-update";
        this.createBucket(bucket);

        var key = upload("trigger/on-update", bucket);

        Trigger trigger = Trigger.builder()
            .id("s3-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue("trigger/on-update"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.evaluate(context.getKey(), context.getValue());

        update(key, bucket);
        Thread.sleep(2000);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));
    }

    @Test
    void shouldExecuteOnCreateOrUpdate() throws Exception {
        String bucket = "trigger-on-create-or-update";
        this.createBucket(bucket);

        Trigger trigger = Trigger.builder()
            .id("s3-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue("trigger/on-create-or-update"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .interval(Duration.ofSeconds(10))
            .build();

        var key = upload("trigger/on-create-or-update", bucket);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> createExecution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat("Trigger should fire on CREATE", createExecution.isPresent(), is(true));

        update(key, bucket);
        Thread.sleep(2000);

        Optional<Execution> updateExecution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(updateExecution.isPresent(), is(true));
    }

    @Test
    void maxFilesExceeded() throws Exception {
        String bucket = "trigger-maxfiles-exceeded";
        this.createBucket(bucket);

        // Upload 5 files
        for (int i = 0; i < 5; i++) {
            upload("trigger/maxfiles", bucket);
        }

        // Trigger with maxFiles=3 (less than 5 files) - should fire with first 3 files (truncated)
        Trigger trigger = Trigger.builder()
            .id("s3-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue("trigger/maxfiles"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .maxFiles(Property.ofValue(3))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());
        // When maxFiles exceeded, List returns first 3 files, so Trigger should fire
        assertThat(execution.isPresent(), is(true));
    }

    @Test
    void maxFilesNotExceeded() throws Exception {
        String bucket = "trigger-maxfiles-ok";
        this.createBucket(bucket);

        // Upload 5 files
        for (int i = 0; i < 5; i++) {
            upload("trigger/maxfiles-ok", bucket);
        }

        // Trigger with maxFiles=10 (more than 5 files) - should fire
        Trigger trigger = Trigger.builder()
            .id("s3-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue("trigger/maxfiles-ok"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .maxFiles(Property.ofValue(10))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));
    }
}

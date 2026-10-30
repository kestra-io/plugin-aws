package io.kestra.plugin.aws.s3;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.aws.s3.models.S3Object;

import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest(startRunner = true, startScheduler = true)
class TriggerTest extends AbstractTest {
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
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();
        Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError ->
        {
            Execution execution = executionWithError.getLeft();

            if (execution.getFlowId().equals("s3-listen")) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        upload("trigger/s3", bucket);
        upload("trigger/s3", bucket);

        repositoryLoader.load(flowWithFlociEndpoint("flows/s3/s3-listen.yaml"));

        boolean await = queueCount.await(15, TimeUnit.SECONDS);
        assertThat(await, is(true));
        receive.blockLast();

        @SuppressWarnings("unchecked")
        java.util.List<S3Object> trigger = (java.util.List<S3Object>) last.get().getTrigger().getVariables().get("objects");

        assertThat(trigger.size(), is(2));

        int remainingFilesOnBucket = listTask.run(runContext(listTask))
            .getObjects()
            .size();
        assertThat(remainingFilesOnBucket, is(0));
    }

    @Test
    void noneAction() throws Exception {
        String bucket = "trigger-none-action-test";
        this.createBucket(bucket);
        List listTask = list().bucket(Property.ofValue(bucket)).build();

        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();
        Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError ->
        {
            Execution execution = executionWithError.getLeft();

            if (execution.getFlowId().equals("s3-listen-none-action")) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        upload("trigger/s3", bucket);
        upload("trigger/s3", bucket);

        repositoryLoader.load(flowWithFlociEndpoint("flows/s3/s3-listen-none-action.yaml"));

        boolean await = queueCount.await(10, TimeUnit.SECONDS);
        assertThat(await, is(true));
        receive.blockLast();

        @SuppressWarnings("unchecked")
        java.util.List<S3Object> trigger = (java.util.List<S3Object>) last.get().getTrigger().getVariables().get("objects");

        assertThat(trigger.size(), is(2));

        int remainingFilesOnBucket = listTask.run(runContext(listTask))
            .getObjects()
            .size();
        assertThat(remainingFilesOnBucket, is(2));
    }

    @Test
    void forcePathStyleWithSimpleLocalhost() throws Exception {
        String bucket = "trigger-force-path-style-test";
        this.createBucket(bucket);
        List listTask = list().bucket(Property.ofValue(bucket)).build();

        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();
        Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError ->
        {
            Execution execution = executionWithError.getLeft();

            if (execution.getFlowId().equals("s3-listen-localhost-force-path-style")) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        upload("trigger/s3", bucket);
        upload("trigger/s3", bucket);

        repositoryLoader.load(flowWithFlociEndpoint("flows/s3/s3-listen-localhost-force-path-style.yaml"));

        boolean await = queueCount.await(15, TimeUnit.SECONDS);
        assertThat("trigger should work with localhost endpoint + forcePathStyle", await, is(true));
        receive.blockLast();

        @SuppressWarnings("unchecked")
        java.util.List<S3Object> trigger = (java.util.List<S3Object>) last.get().getTrigger().getVariables().get("objects");

        assertThat(trigger.size(), is(2));

        int remainingFilesOnBucket = listTask.run(runContext(listTask))
            .getObjects()
            .size();
        assertThat(remainingFilesOnBucket, is(0));
    }

    private File flowWithFlociEndpoint(String resource) throws Exception {
        String yaml = new String(TriggerTest.class.getClassLoader().getResourceAsStream(resource).readAllBytes());
        yaml = yaml.replace("http://localhost:4566", endpointUrl())
            .replace("http://127.0.0.1:4566", endpointUrl());
        File tempFlow = File.createTempFile("s3-trigger", ".yaml");
        Files.writeString(tempFlow.toPath(), yaml);
        return tempFlow;
    }

    @Test
    void shouldExecuteOnCreate() throws Exception {
        String bucket = "trigger-on-create";
        this.createBucket(bucket);

        Trigger trigger = Trigger.builder()
            .id("s3-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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

    @Test
    void failedMoveIsRetriedOnNextPoll() throws Exception {
        String bucket = "trigger-move-retry";
        String archive = "trigger-move-retry-archive";
        this.createBucket(bucket);
        upload("trigger/move-retry", bucket);
        upload("trigger/move-retry", bucket);

        Trigger trigger = Trigger.builder()
            .id("s3-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue("trigger/move-retry"))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveTo(Copy.CopyObject.builder().bucket(Property.ofValue(archive)).key(Property.ofValue("archived")).build())
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        // The archive bucket does not exist yet, so the MOVE fails for every object.
        var failure = assertThrows(Exception.class, () -> trigger.evaluate(context.getKey(), context.getValue()));
        assertThat(causeChain(failure).stream().anyMatch(NoSuchBucketException.class::isInstance), is(true));

        this.createBucket(archive);

        // Once the cause is fixed, the next poll picks the objects up again, fires and moves them.
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));
        assertThat(triggeredKeys(execution.get()).size(), is(2));
        assertThat(keys(bucket, "trigger/move-retry").size(), is(0));
        assertThat(keys(archive, "archived").size(), is(2));
    }

    @Test
    void partlyFailedDeleteFiresTheOthersAndRetriesTheFailedOne() throws Exception {
        String bucket = "trigger-partial-delete";
        this.createBucket(bucket);
        upload("trigger/partial", bucket);
        upload("trigger/partial", bucket);
        var uploaded = keys(bucket, "trigger/partial");
        var failingKey = uploaded.get(1);

        Trigger trigger = Trigger.builder()
            .id("s3-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue("trigger/partial"))
            .action(Property.ofValue(ActionInterface.Action.DELETE))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        // The delete fails for one of the two objects only.
        Optional<Execution> first;
        try (
            var ignored = Mockito.mockStatic(S3Service.class, invocation ->
            {
                if (
                    invocation.getMethod().getName().equals("performAction")
                        && invocation.<java.util.List<S3Object>> getArgument(0).getFirst().getKey().equals(failingKey)
                ) {
                    throw new IllegalStateException("simulated AccessDenied");
                }
                return invocation.callRealMethod();
            })
        ) {
            first = trigger.evaluate(context.getKey(), context.getValue());
        }

        assertThat(first.isPresent(), is(true));
        assertThat(triggeredKeys(first.get()), is(java.util.List.of(uploaded.getFirst())));
        assertThat(keys(bucket, "trigger/partial"), is(java.util.List.of(failingKey)));

        // The failed object was not recorded as seen, so the next poll fires and deletes it.
        Optional<Execution> second = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(second.isPresent(), is(true));
        assertThat(triggeredKeys(second.get()), is(java.util.List.of(failingKey)));
        assertThat(keys(bucket, "trigger/partial").size(), is(0));
    }

    @Test
    void killDuringActionsStillFiresTheActionedObjectsAndRetriesTheRest() throws Exception {
        String bucket = "trigger-kill-mid-actions";
        this.createBucket(bucket);
        upload("trigger/kill", bucket);
        upload("trigger/kill", bucket);
        var uploaded = keys(bucket, "trigger/kill");

        var triggerId = "s3-" + IdUtils.create();
        var stateKey = "kill-" + triggerId;
        java.util.function.Supplier<Trigger> newTrigger = () -> Trigger.builder()
            .id(triggerId)
            .type(Trigger.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue("trigger/kill"))
            .action(Property.ofValue(ActionInterface.Action.DELETE))
            .stateKey(Property.ofValue(stateKey))
            .interval(Duration.ofSeconds(10))
            .build();

        var trigger = newTrigger.get();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        // The trigger is killed right after the first object was deleted.
        var actioned = new AtomicReference<String>();
        Optional<Execution> first;
        try (
            var ignored = Mockito.mockStatic(S3Service.class, invocation ->
            {
                var result = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("performAction")) {
                    actioned.compareAndSet(null, invocation.<java.util.List<S3Object>> getArgument(0).getFirst().getKey());
                    trigger.kill();
                }
                return result;
            })
        ) {
            first = trigger.evaluate(context.getKey(), context.getValue());
        }

        // The deleted object still fires and is recorded, the other one is left untouched.
        assertThat(first.isPresent(), is(true));
        assertThat(triggeredKeys(first.get()), is(java.util.List.of(actioned.get())));
        var remaining = uploaded.stream().filter(key -> !key.equals(actioned.get())).toList();
        assertThat(keys(bucket, "trigger/kill"), is(remaining));

        // A fresh evaluation (same state) only picks up the object that was not processed.
        var restarted = newTrigger.get();
        var restartedContext = TestsUtils.mockTrigger(runContextFactory, restarted);
        Optional<Execution> second = restarted.evaluate(restartedContext.getKey(), restartedContext.getValue());
        assertThat(second.isPresent(), is(true));
        assertThat(triggeredKeys(second.get()), is(remaining));
        assertThat(keys(bucket, "trigger/kill").size(), is(0));
    }

    private static java.util.List<Throwable> causeChain(Throwable throwable) {
        var chain = new java.util.ArrayList<Throwable>();
        for (var current = throwable; current != null && !chain.contains(current); current = current.getCause()) {
            chain.add(current);
        }
        return chain;
    }

    @SuppressWarnings("unchecked")
    private static java.util.List<String> triggeredKeys(Execution execution) {
        return ((java.util.List<Map<String, Object>>) execution.getTrigger().getVariables().get("objects")).stream()
            .map(object -> (String) object.get("key"))
            .toList();
    }

    private java.util.List<String> keys(String bucket, String prefix) throws Exception {
        List listTask = list().bucket(Property.ofValue(bucket)).prefix(Property.ofValue(prefix)).build();
        return listTask.run(runContext(listTask)).getObjects().stream().map(S3Object::getKey).sorted().toList();
    }
}

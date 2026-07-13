package io.kestra.plugin.aws.sqs;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.plugin.aws.sqs.model.Message;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true, startScheduler = true)
class RealtimeTriggerTest extends AbstractSqsTest {
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Test
    void flow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> lastExecution = new AtomicReference<>();
        executionQueue.addListener(execution -> {
            lastExecution.set(execution);
            queueCount.countDown();
            assertThat(execution.getFlowId(), is("realtime"));
        });

        String yaml = """
            id: realtime
            namespace: io.kestra.tests

            triggers:
              - id: watch
                type: io.kestra.plugin.aws.sqs.RealtimeTrigger
                endpointOverride: "%s"
                queueUrl: "%s"
                region: "us-east-1"
                accessKeyId: "accesskey"
                secretKeyId: "secretkey"

            tasks:
              - id: end
                type: io.kestra.plugin.core.debug.Return
                format: "{{task.id}} > {{taskrun.startDate}}"
            """.formatted(endpointUrl(), queueUrl());
        File tempFlow = File.createTempFile("sqs-realtime", ".yaml");
        Files.writeString(tempFlow.toPath(), yaml);
        repositoryLoader.load(tempFlow);

        Publish task = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(
                List.of(
                    Message.builder().data("Hello World").build()
                )
            )
            .build();

        var runContext = runContextFactory.of();

        task.run(runContext);

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        Execution last = lastExecution.get();
        assertThat(last.getTrigger().getVariables().size(), is(1));
        assertThat(last.getTrigger().getVariables().get("data"), is("Hello World"));
    }

}

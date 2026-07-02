package io.kestra.plugin.aws.sqs;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.aws.sqs.model.Message;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true, startScheduler = true)
@org.junit.jupiter.api.parallel.Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("kestra-sqs-trigger")
class RealtimeTriggerTest extends AbstractSqsTest {
    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Test
    void flow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        Flux<Execution> receive = TestsUtils.receive(executionQueue, execution ->
        {
            if (execution.isLeft() && "realtime".equals(execution.getLeft().getFlowId()) && "io.kestra.tests".equals(execution.getLeft().getNamespace())) {
                queueCount.countDown();
            }
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

        task.run(runContextFactory.of());

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        Execution last = receive.blockLast();
        assertThat(last.getTrigger().getVariables().size(), is(1));
        assertThat(last.getTrigger().getVariables().get("data"), is("Hello World"));
    }

}

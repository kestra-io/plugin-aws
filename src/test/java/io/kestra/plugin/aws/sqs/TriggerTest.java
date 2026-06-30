package io.kestra.plugin.aws.sqs;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.aws.sqs.model.Message;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest(startRunner = true, startScheduler = true)
@ResourceLock("kestra-sqs-trigger")
class TriggerTest extends AbstractSqsTest {
    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void flow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        Flux<Execution> receive = TestsUtils.receive(executionQueue, execution ->
        {
            queueCount.countDown();
            assertThat(execution.getLeft().getFlowId(), is("sqs-listen"));
        });

        String yaml = """
            id: sqs-listen
            namespace: io.kestra.tests

            triggers:
              - id: watch
                type: io.kestra.plugin.aws.sqs.Trigger
                endpointOverride: "%s"
                queueUrl: "%s"
                region: "us-east-1"
                accessKeyId: "accesskey"
                secretKeyId: "secretkey"
                maxRecords: 2
                interval: PT10S

            tasks:
              - id: end
                type: io.kestra.plugin.core.debug.Return
                format: "{{task.id}} > {{taskrun.startDate}}"
            """.formatted(endpointUrl(), queueUrl());
        File tempFlow = File.createTempFile("sqs-listen", ".yaml");
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
                    Message.builder().data("Hello World").build(),
                    Message.builder().data("Hello Kestra").delaySeconds(5).build()
                )
            )
            .build();

        task.run(runContextFactory.of());

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        Execution last = receive.blockLast();
        var count = (Integer) last.getTrigger().getVariables().get("count");
        var uri = (String) last.getTrigger().getVariables().get("uri");
        assertThat(count, is(2));
        assertThat(uri, is(notNullValue()));
    }

}

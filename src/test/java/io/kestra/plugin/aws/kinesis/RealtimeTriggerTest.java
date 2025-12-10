package io.kestra.plugin.aws.kinesis;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.aws.kinesis.model.Record;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.kinesis.model.*;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class RealtimeTriggerTest extends AbstractKinesisTest {
    @Inject
    ApplicationContext applicationContext;

    @Inject
    FlowListeners flowListeners;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    QueueInterface<Execution> executionQueue;

    @Inject
    LocalFlowRepositoryLoader repositoryLoader;

    static String consumerArn;

    @Test
    void flow() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Flux<Execution> received = TestsUtils.receive(executionQueue, e -> latch.countDown());

        DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, UUID.randomUUID().toString(), 8, null);
        try (AbstractScheduler scheduler = new JdbcScheduler(applicationContext, flowListeners)) {

            worker.run();
            scheduler.run();

            String yaml = """
                id: realtime
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.data }}"

                triggers:
                  - id: realtime
                    type: io.kestra.plugin.aws.kinesis.RealtimeTrigger
                    streamName: "stream"
                    consumerArn: "%s"
                    region: "us-east-1"
                    accessKeyId: "test"
                    secretKeyId: "test"
                    endpointOverride: "http://localhost:4566"
                    iteratorType: TRIM_HORIZON
                """
                .formatted(consumerArn);


            File tempFlow = File.createTempFile("kinesis-realtime", ".yaml");
            Files.writeString(tempFlow.toPath(), yaml);

            repositoryLoader.load(tempFlow);

            Record record = Record.builder()
                .partitionKey("pk")
                .data("hello")
                .build();

            var put = PutRecords.builder()
                .endpointOverride(Property.ofValue(localstack.getEndpoint().toString()))
                .region(Property.ofValue(localstack.getRegion()))
                .accessKeyId(Property.ofValue(localstack.getAccessKey()))
                .secretKeyId(Property.ofValue(localstack.getSecretKey()))
                .streamName(Property.ofValue("stream"))
                .records(List.of(record))
                .build();

            put.run(runContextFactory.of());

            boolean done = latch.await(30, TimeUnit.SECONDS);
            assertThat(done, is(true));

            Execution exec = received.blockLast();
            assertThat(exec.getTrigger().getVariables().get("data"), is("hello"));
        }
    }
}
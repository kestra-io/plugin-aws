package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.aws.sqs.model.Message;
import io.kestra.plugin.aws.sqs.model.SerdeType;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PublishThenConsumeTest extends AbstractSqsTest {
    @Test
    void runText() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString()))
            .queueUrl(Property.of(queueUrl()))
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .from(
                List.of(
                    Message.builder().data("Hello World").build(),
                    Message.builder().data("Hello Kestra").delaySeconds(5).build()
                )
            )
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(2));

        var consume = Consume.builder()
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString()))
            .queueUrl(Property.of(queueUrl()))
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .maxRecords(Property.of(2))
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(2));
    }

    @Test
    void runJson() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString()))
            .queueUrl(Property.of(queueUrl()))
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .from(
                List.of(
                    Message.builder().data("""
                        {"hello" : "world"}""").build(),
                    Message.builder().data("""
                        {"hello" : "kestra"}""").delaySeconds(5).build()
                )
            )
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(2));

        var consume = Consume.builder()
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString()))
            .queueUrl(Property.of(queueUrl()))
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .serdeType(Property.of(SerdeType.JSON))
            .maxRecords(Property.of(2))
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(2));
    }
}
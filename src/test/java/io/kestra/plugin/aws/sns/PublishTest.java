package io.kestra.plugin.aws.sns;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.aws.sns.model.Message;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PublishTest extends AbstractSnsTest {
    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.SNS).toString()))
            .topicArn(Property.ofValue(TOPIC_ARN))
            .region(Property.ofValue(localstack.getRegion()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .from(
                List.of(
                    Message.builder().data("Hello World").build(),
                    Message.builder().data("Hello Kestra").subject("Kestra").build()
                )
            )
            .build();

        var client = publish.client(runContext);
        createTopic(client);

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(2));
    }
}

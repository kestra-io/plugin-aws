package io.kestra.plugin.aws.sns;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.aws.sns.model.Message;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PublishTest extends AbstractSnsTest {
    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .topicArn(Property.ofValue(TOPIC_ARN))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
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

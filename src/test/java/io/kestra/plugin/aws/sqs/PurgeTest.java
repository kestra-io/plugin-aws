package io.kestra.plugin.aws.sqs;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.Await;
import io.kestra.plugin.aws.sqs.model.Message;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Execution(ExecutionMode.SAME_THREAD)
class PurgeTest extends AbstractSqsTest {
    @Test
    void purgeEmptiesTheQueue() throws Exception {
        var publish = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(List.of(
                Message.builder().data("first").build(),
                Message.builder().data("second").build(),
                Message.builder().data("third").build()
            ))
            .build();
        assertThat(publish.run(runContextFactory.of()).getMessagesCount(), is(3));

        try (var sqsClient = sqsClient()) {
            Await.until(() -> visibleMessages(sqsClient) == 3, Duration.ofMillis(500), Duration.ofSeconds(20));

            var purge = Purge.builder()
                .endpointOverride(Property.ofValue(endpointUrl()))
                .queueUrl(Property.ofValue(queueUrl()))
                .region(Property.ofValue(REGION))
                .accessKeyId(Property.ofValue(ACCESS_KEY))
                .secretKeyId(Property.ofValue(SECRET_KEY))
                .build();
            var output = purge.run(runContextFactory.of());

            assertThat(output.getQueueUrl(), is(queueUrl()));
            Await.until(() -> visibleMessages(sqsClient) == 0, Duration.ofMillis(500), Duration.ofSeconds(20));
        }
    }

    private int visibleMessages(SqsClient sqsClient) {
        return Integer.parseInt(
            sqsClient.getQueueAttributes(
                GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl())
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                    .build()
            ).attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
        );
    }
}

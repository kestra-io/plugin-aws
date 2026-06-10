package io.kestra.plugin.aws.sqs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.aws.sqs.model.Message;
import io.kestra.plugin.aws.sqs.model.SerdeType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Tests RealtimeTrigger.publisher() JSON deserialization in isolation,
 * without starting the full Kestra runner/scheduler to avoid inter-test
 * message consumption by other active triggers.
 */
@KestraTest
class RealtimeTriggerPublisherTest extends AbstractSqsTest {

    @Test
    void publisherWithJsonSerdeTypeDeserializesBody() throws Exception {
        var runContext = runContextFactory.of();

        Publish publish = Publish.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .from(List.of(Message.builder().data("{\"order_id\":\"42\",\"customer\":\"Alice\"}").build()))
            .build();
        publish.run(runContext);

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("test-rt-json")
            .type(RealtimeTrigger.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .build();

        Consume task = Consume.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .queueUrl(Property.ofValue(queueUrl()))
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .build();

        CountDownLatch received = new CountDownLatch(1);
        var messages = new CopyOnWriteArrayList<Message>();

        trigger.publisher(task, runContext)
            .subscribe(msg -> {
                messages.add(msg);
                received.countDown();
                trigger.stop();
            });

        boolean got = received.await(1, TimeUnit.MINUTES);
        assertThat("timed out waiting for JSON message from publisher", got, is(true));
        assertThat(messages.size(), is(1));
        var data = messages.getFirst().getData();
        assertThat(data, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        var map = (Map<String, Object>) data;
        assertThat(map.get("order_id"), is("42"));
        assertThat(map.get("customer"), is("Alice"));
    }
}

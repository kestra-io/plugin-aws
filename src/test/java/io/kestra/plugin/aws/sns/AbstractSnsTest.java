package io.kestra.plugin.aws.sns;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;

@KestraTest
@Testcontainers
public class AbstractSnsTest extends AbstractLocalStackTest {
    static final String TOPIC_ARN = "arn:aws:sns:us-east-1:000000000000:test-topic";

    @Inject
    protected RunContextFactory runContextFactory;

    void createTopic(SnsClient client) {
        if (!client.listTopics().topics().contains(TOPIC_ARN)) {
            client.createTopic(CreateTopicRequest.builder().name("test-topic").build());
        }
    }
}

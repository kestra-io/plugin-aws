package io.kestra.plugin.aws.sns;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;

@MicronautTest
@Testcontainers
public class AbstractSnsTest {
    static final String TOPIC_ARN = "arn:aws:sns:us-east-1:000000000000:test-topic";

    protected static LocalStackContainer localstack;

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.3.1"))
            .withServices(LocalStackContainer.Service.SNS);
        localstack.start();
    }

    @AfterAll
    static void stopLocalstack() {
        if(localstack != null) {
            localstack.stop();
        }
    }

    void createTopic(SnsClient client) {
        if(!client.listTopics().topics().contains(TOPIC_ARN)) {
            client.createTopic(CreateTopicRequest.builder().name("test-topic").build());
        }
    }
}

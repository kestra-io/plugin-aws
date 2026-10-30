package io.kestra.plugin.aws.sqs;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.PurgeQueueInProgressException;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.PurgeQueueResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class PurgeUnitTest {
    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/000000000000/unit-queue";

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void purgesTheRenderedQueueAndReturnsItsUrl() throws Exception {
        var mockClient = mock(SqsClient.class);
        when(mockClient.purgeQueue(any(PurgeQueueRequest.class))).thenReturn(PurgeQueueResponse.builder().build());

        var output = withClient(purge().build(), mockClient).run(runContextFactory.of());

        var request = ArgumentCaptor.forClass(PurgeQueueRequest.class);
        verify(mockClient, times(1)).purgeQueue(request.capture());
        assertThat(request.getValue().queueUrl(), is(QUEUE_URL));
        assertThat(output.getQueueUrl(), is(QUEUE_URL));
        verify(mockClient).close();
    }

    @Test
    void purgeInProgressFailsWithAnExplicitMessage() throws Exception {
        var mockClient = mock(SqsClient.class);
        var inProgress = PurgeQueueInProgressException.builder().message("Only one PurgeQueue operation on unit-queue is allowed every 60 seconds.").build();
        when(mockClient.purgeQueue(any(PurgeQueueRequest.class))).thenThrow(inProgress);

        var task = withClient(purge().build(), mockClient);
        var thrown = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

        assertThat(thrown.getMessage(), containsString(QUEUE_URL));
        assertThat(thrown.getMessage(), containsString("60 seconds"));
        assertThat(thrown.getCause(), sameInstance(inProgress));
        verify(mockClient).close();
    }

    @Test
    void missingQueueUrlFailsBeforeAnyCall() throws Exception {
        assertFailsBeforeAnyCall(null);
    }

    @Test
    void blankQueueUrlFailsBeforeAnyCall() throws Exception {
        assertFailsBeforeAnyCall(Property.ofValue(""));
    }

    private void assertFailsBeforeAnyCall(Property<String> queueUrl) throws Exception {
        var mockClient = mock(SqsClient.class);
        var task = withClient(purge().queueUrl(queueUrl).build(), mockClient);

        var thrown = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));

        assertThat(thrown.getMessage(), containsString("queueUrl"));
        verify(mockClient, never()).purgeQueue(any(PurgeQueueRequest.class));
    }

    private static Purge.PurgeBuilder<?, ?> purge() {
        return Purge.builder()
            .queueUrl(Property.ofValue(QUEUE_URL))
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test"))
            .secretKeyId(Property.ofValue("test"));
    }

    private static Purge withClient(Purge task, SqsClient mockClient) throws Exception {
        var spied = spy(task);
        doReturn(mockClient).when(spied).client(any());
        return spied;
    }
}

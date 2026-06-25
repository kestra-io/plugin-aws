package io.kestra.plugin.aws.bedrock;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class ConverseStreamTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenValidMessages_whenConverseStream_thenAccumulatesContent() throws Exception {
        RunContext runContext = runContextFactory.of();

        ConverseStream task = ConverseStream.builder()
            .id("test-converse-stream")
            .type(ConverseStream.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .modelId(Property.ofValue("anthropic.claude-3-5-sonnet-20241022-v2:0"))
            .messages(Property.ofValue(List.of(Map.of("role", "user", "content", "Hello!"))))
            .system(Property.ofValue("You are a helpful assistant."))
            .inferenceConfig(Property.ofValue(Map.of("maxTokens", 512)))
            .build();

        BedrockRuntimeAsyncClient mockClient = mock(BedrockRuntimeAsyncClient.class);
        when(mockClient.converseStream(
            any(ConverseStreamRequest.class),
            any(ConverseStreamResponseHandler.class))
        ).thenAnswer(invocation -> {
            ConverseStreamResponseHandler handler = invocation.getArgument(1);

            // Deliver synthetic events through the handler's event stream
            handler.onEventStream(subscriber -> {
                ContentBlockDeltaEvent delta1 = ContentBlockDeltaEvent.builder()
                    .delta(ContentBlockDelta.fromText("Hello"))
                    .build();
                ContentBlockDeltaEvent delta2 = ContentBlockDeltaEvent.builder()
                    .delta(ContentBlockDelta.fromText("! How can I help?"))
                    .build();
                MessageStopEvent stopEvent = MessageStopEvent.builder()
                    .stopReason(StopReason.END_TURN)
                    .build();
                ConverseStreamMetadataEvent metaEvent = ConverseStreamMetadataEvent.builder()
                    .usage(TokenUsage.builder().inputTokens(5).outputTokens(8).build())
                    .build();

                subscriber.onSubscribe(new org.reactivestreams.Subscription() {
                    @Override public void request(long n) {
                        subscriber.onNext(delta1);
                        subscriber.onNext(delta2);
                        subscriber.onNext(stopEvent);
                        subscriber.onNext(metaEvent);
                        subscriber.onComplete();
                    }
                    @Override public void cancel() {}
                });
            });

            return CompletableFuture.completedFuture(null);
        });

        ConverseStream spy = spy(task);
        doReturn(mockClient).when(spy).asyncClient(any(RunContext.class));

        ConverseStream.Output output = spy.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getModelId(), is("anthropic.claude-3-5-sonnet-20241022-v2:0"));
        assertThat(output.getContent(), is("Hello! How can I help?"));
        assertThat(output.getStopReason(), is("end_turn"));
        assertThat(output.getInputTokens(), is(5));
        assertThat(output.getOutputTokens(), is(8));
    }

    @Test
    void givenInvalidRole_whenConverseStream_thenThrowsIllegalArgumentException() throws Exception {
        RunContext runContext = runContextFactory.of();

        ConverseStream task = ConverseStream.builder()
            .id("test-converse-stream-bad-role")
            .type(ConverseStream.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .modelId(Property.ofValue("anthropic.claude-3-5-sonnet-20241022-v2:0"))
            .messages(Property.ofValue(List.of(Map.of("role", "system", "content", "You are a bot."))))
            .build();

        BedrockRuntimeAsyncClient mockClient = mock(BedrockRuntimeAsyncClient.class);
        ConverseStream spy = spy(task);
        doReturn(mockClient).when(spy).asyncClient(any(RunContext.class));

        assertThrows(IllegalArgumentException.class, () -> spy.run(runContext));
    }
}

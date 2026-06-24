package io.kestra.plugin.aws.bedrock;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.*;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class ConverseTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenValidMessages_whenConverse_thenOutputContainsContent() throws Exception {
        RunContext runContext = runContextFactory.of();

        Converse task = Converse.builder()
            .id("test-converse")
            .type(Converse.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .modelId(Property.ofValue("anthropic.claude-3-5-sonnet-20241022-v2:0"))
            .messages(Property.ofValue(List.of(Map.of("role", "user", "content", "Hello!"))))
            .system(Property.ofValue("You are a helpful assistant."))
            .inferenceConfig(Property.ofValue(Map.of("maxTokens", 512)))
            .build();

        ContentBlock textBlock = ContentBlock.fromText("Hello! How can I help you today?");
        Message assistantMessage = Message.builder()
            .role(ConversationRole.ASSISTANT)
            .content(textBlock)
            .build();

        ConverseOutput converseOutput = ConverseOutput.builder()
            .message(assistantMessage)
            .build();

        TokenUsage usage = TokenUsage.builder()
            .inputTokens(10)
            .outputTokens(15)
            .build();

        ConverseResponse mockResponse = ConverseResponse.builder()
            .output(converseOutput)
            .stopReason(StopReason.END_TURN)
            .usage(usage)
            .build();

        BedrockRuntimeClient mockClient = mock(BedrockRuntimeClient.class);
        when(mockClient.converse(any(ConverseRequest.class))).thenReturn(mockResponse);

        Converse spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        Converse.Output output = spy.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getModelId(), is("anthropic.claude-3-5-sonnet-20241022-v2:0"));
        assertThat(output.getContent(), is("Hello! How can I help you today?"));
        assertThat(output.getStopReason(), is("end_turn"));
        assertThat(output.getInputTokens(), is(10));
        assertThat(output.getOutputTokens(), is(15));
    }
}

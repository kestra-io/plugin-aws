package io.kestra.plugin.aws.bedrock;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@MicronautTest
class InvokeModelTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenValidPayload_whenInvoked_thenOutputContainsBody() throws Exception {
        RunContext runContext = runContextFactory.of();

        InvokeModel task = InvokeModel.builder()
            .id("test-invoke-model")
            .type(InvokeModel.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .modelId(Property.ofValue("amazon.titan-text-express-v1"))
            .body(Property.ofValue(Map.of("inputText", "Summarize Kafka in 3 bullets.")))
            .build();

        String fakeResponse = "{\"results\":[{\"outputText\":\"1. High throughput\\n2. Durable\\n3. Scalable\"}]}";
        InvokeModelResponse mockResponse = InvokeModelResponse.builder()
            .body(SdkBytes.fromUtf8String(fakeResponse))
            .build();

        BedrockRuntimeClient mockClient = mock(BedrockRuntimeClient.class);
        when(mockClient.invokeModel(any(software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest.class)))
            .thenReturn(mockResponse);

        InvokeModel spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        InvokeModel.Output output = spy.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getModelId(), is("amazon.titan-text-express-v1"));
        assertThat(output.getBody(), is(notNullValue()));
        assertThat(output.getBody().containsKey("results"), is(true));
    }
}

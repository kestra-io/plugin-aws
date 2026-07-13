package io.kestra.plugin.aws.bedrock;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.bedrock.BedrockClient;
import software.amazon.awssdk.services.bedrock.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class ListFoundationModelsTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenNoFilters_whenList_thenReturnsAllModels() throws Exception {
        RunContext runContext = runContextFactory.of();

        ListFoundationModels task = ListFoundationModels.builder()
            .id("test-list-models")
            .type(ListFoundationModels.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .build();

        FoundationModelSummary model1 = FoundationModelSummary.builder()
            .modelId("amazon.titan-text-express-v1")
            .modelName("Titan Text G1 - Express")
            .providerName("Amazon")
            .outputModalities(ModelModality.TEXT)
            .inferenceTypesSupported(InferenceType.ON_DEMAND)
            .build();

        FoundationModelSummary model2 = FoundationModelSummary.builder()
            .modelId("anthropic.claude-3-5-sonnet-20241022-v2:0")
            .modelName("Claude 3.5 Sonnet v2")
            .providerName("Anthropic")
            .outputModalities(ModelModality.TEXT)
            .inferenceTypesSupported(InferenceType.ON_DEMAND)
            .build();

        ListFoundationModelsResponse mockResponse = ListFoundationModelsResponse.builder()
            .modelSummaries(List.of(model1, model2))
            .build();

        BedrockClient mockClient = mock(BedrockClient.class);
        when(mockClient.listFoundationModels(any(ListFoundationModelsRequest.class))).thenReturn(mockResponse);

        ListFoundationModels spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        ListFoundationModels.Output output = spy.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getModels(), hasSize(2));
        assertThat(output.getModels().get(0).get("modelId"), is("amazon.titan-text-express-v1"));
        assertThat(output.getModels().get(1).get("providerName"), is("Anthropic"));
    }

    @Test
    void givenProviderFilter_whenList_thenFilterIsApplied() throws Exception {
        RunContext runContext = runContextFactory.of();

        ListFoundationModels task = ListFoundationModels.builder()
            .id("test-list-models-filtered")
            .type(ListFoundationModels.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .byProvider(Property.ofValue("anthropic"))
            .byOutputModality(Property.ofValue("TEXT"))
            .build();

        FoundationModelSummary model = FoundationModelSummary.builder()
            .modelId("anthropic.claude-3-5-sonnet-20241022-v2:0")
            .modelName("Claude 3.5 Sonnet v2")
            .providerName("Anthropic")
            .outputModalities(ModelModality.TEXT)
            .inferenceTypesSupported(InferenceType.ON_DEMAND)
            .build();

        ListFoundationModelsResponse mockResponse = ListFoundationModelsResponse.builder()
            .modelSummaries(List.of(model))
            .build();

        BedrockClient mockClient = mock(BedrockClient.class);
        when(mockClient.listFoundationModels(any(ListFoundationModelsRequest.class))).thenReturn(mockResponse);

        ListFoundationModels spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        ListFoundationModels.Output output = spy.run(runContext);

        assertThat(output.getModels(), hasSize(1));
        assertThat(output.getModels().get(0).get("modelId"), is("anthropic.claude-3-5-sonnet-20241022-v2:0"));
    }
}

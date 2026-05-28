package io.kestra.plugin.aws.emr;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.emr.models.StepConfig;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsRequest;
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsResponse;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class SubmitStepsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void executionRoleArnIsForwardedToRequest() throws Exception {
        RunContext runContext = runContextFactory.of();
        EmrClient emrClient = mock(EmrClient.class);
        when(emrClient.addJobFlowSteps(any(Consumer.class)))
            .thenReturn(AddJobFlowStepsResponse.builder().stepIds("s-111").build());

        SubmitSteps task = spy(
            SubmitSteps.builder()
                .region(Property.ofValue("eu-west-3"))
                .clusterId(Property.ofValue("j-CLUSTER"))
                .executionRoleArn(Property.ofValue("arn:aws:iam::123456789012:role/StepRole"))
                .steps(List.of(StepConfig.builder()
                    .jar(Property.ofValue("command-runner.jar"))
                    .name(Property.ofValue("step"))
                    .commands(Property.ofValue(List.of("echo hello")))
                    .actionOnFailure(Property.ofValue(StepConfig.Action.CONTINUE))
                    .build()))
                .build()
        );
        doReturn(emrClient).when(task).emrClient(any(RunContext.class));

        task.run(runContext);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<AddJobFlowStepsRequest.Builder>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(emrClient).addJobFlowSteps(captor.capture());

        AddJobFlowStepsRequest.Builder builder = AddJobFlowStepsRequest.builder();
        captor.getValue().accept(builder);
        AddJobFlowStepsRequest req = builder.build();

        assertThat(req.jobFlowId(), is("j-CLUSTER"));
        assertThat(req.executionRoleArn(), is("arn:aws:iam::123456789012:role/StepRole"));
        assertThat(req.steps(), hasSize(1));
    }

    @Test
    void allStepsAreForwardedInOrder() throws Exception {
        RunContext runContext = runContextFactory.of();
        EmrClient emrClient = mock(EmrClient.class);
        when(emrClient.addJobFlowSteps(any(Consumer.class)))
            .thenReturn(AddJobFlowStepsResponse.builder().stepIds("s-1", "s-2").build());

        SubmitSteps task = spy(
            SubmitSteps.builder()
                .region(Property.ofValue("eu-west-3"))
                .clusterId(Property.ofValue("j-CLUSTER"))
                .steps(List.of(
                    StepConfig.builder()
                        .jar(Property.ofValue("command-runner.jar"))
                        .name(Property.ofValue("step-1"))
                        .commands(Property.ofValue(List.of("echo step1")))
                        .actionOnFailure(Property.ofValue(StepConfig.Action.CONTINUE))
                        .build(),
                    StepConfig.builder()
                        .jar(Property.ofValue("command-runner.jar"))
                        .name(Property.ofValue("step-2"))
                        .commands(Property.ofValue(List.of("echo step2")))
                        .actionOnFailure(Property.ofValue(StepConfig.Action.TERMINATE_CLUSTER))
                        .build()
                ))
                .build()
        );
        doReturn(emrClient).when(task).emrClient(any(RunContext.class));

        task.run(runContext);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<AddJobFlowStepsRequest.Builder>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(emrClient).addJobFlowSteps(captor.capture());

        AddJobFlowStepsRequest.Builder builder = AddJobFlowStepsRequest.builder();
        captor.getValue().accept(builder);
        AddJobFlowStepsRequest req = builder.build();

        assertThat(req.steps(), hasSize(2));
        assertThat(req.steps().get(0).name(), is("step-1"));
        assertThat(req.steps().get(1).name(), is("step-2"));
    }
}

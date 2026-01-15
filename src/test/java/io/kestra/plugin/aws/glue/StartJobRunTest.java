package io.kestra.plugin.aws.glue;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.glue.model.Output;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class StartJobRunTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testStartJobRunNoWait_mocked() throws Exception {
        GlueClient glueClient = mock(GlueClient.class);

        when(glueClient.startJobRun(any(StartJobRunRequest.class)))
            .thenReturn(StartJobRunResponse.builder()
                .jobRunId("jr-123")
                .build()
            );

        when(glueClient.getJobRun(any(GetJobRunRequest.class)))
            .thenReturn(GetJobRunResponse.builder()
                .jobRun(JobRun.builder()
                    .jobRunState(JobRunState.RUNNING)
                    .startedOn(java.time.Instant.now())
                    .executionTime(0)
                    .attempt(1)
                    .timeout(10)
                    .build()
                )
                .build()
            );

        StartJobRun task = spy(StartJobRun.builder()
            .region(Property.ofValue("eu-west-3"))
            .jobName(Property.ofValue("my-glue-job"))
            .wait(Property.ofValue(false))
            .build());

        doReturn(glueClient).when(task).glueClient(any());

        Output output = task.run(runContextFactory.of());

        assertNotNull(output);
        assertEquals("jr-123", output.getJobRunId());
        assertEquals("my-glue-job", output.getJobName());

        verify(glueClient).startJobRun(any(StartJobRunRequest.class));
    }
}

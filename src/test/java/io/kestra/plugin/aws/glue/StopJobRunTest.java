package io.kestra.plugin.aws.glue;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.glue.model.Output;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@KestraTest
class StopJobRunTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void testStopJobRunNoWait_mocked() throws Exception {
        var runContext = runContextFactory.of();

        GlueClient glueClient = mock(GlueClient.class);

        when(glueClient.batchStopJobRun(any(BatchStopJobRunRequest.class)))
            .thenReturn(BatchStopJobRunResponse.builder().build());

        when(glueClient.getJobRun(any(GetJobRunRequest.class)))
            .thenReturn(
                GetJobRunResponse.builder()
                    .jobRun(
                        JobRun.builder()
                            .jobRunState(JobRunState.STOPPING)
                            .startedOn(java.time.Instant.now())
                            .executionTime(0)
                            .attempt(1)
                            .timeout(10)
                            .build()
                    )
                    .build()
            );

        when(glueClient.getJobRun(any(GetJobRunRequest.class)))
            .thenReturn(
                GetJobRunResponse.builder()
                    .jobRun(JobRun.builder()
                        .jobRunState(JobRunState.STOPPED)
                        .startedOn(Instant.now())
                        .executionTime(0)
                        .attempt(1)
                        .timeout(10)
                        .build())
                    .build()
            );

        StopJobRun task = spy(StopJobRun.builder()
            .region(Property.ofValue("eu-west-3"))
            .jobName(Property.ofValue("my-glue-job"))
            .jobRunId(Property.ofValue("jr-123"))
            .wait(Property.ofValue(false))
            .build());

        doReturn(glueClient).when(task).glueClient(any());

        Output output = task.run(runContext);

        assertNotNull(output);
        assertEquals("my-glue-job", output.getJobName());
        assertEquals("jr-123", output.getJobRunId());
        assertNotNull(output.getState());

        verify(glueClient).batchStopJobRun(any(BatchStopJobRunRequest.class));
        verify(glueClient).getJobRun(any(GetJobRunRequest.class));
    }

    @Test
    void testStopJobRunWithWait_mocked() throws Exception {
        var runContext = runContextFactory.of();

        GlueClient glueClient = mock(GlueClient.class);

        when(glueClient.batchStopJobRun(any(BatchStopJobRunRequest.class)))
            .thenReturn(BatchStopJobRunResponse.builder().build());

        GetJobRunResponse stopping = GetJobRunResponse.builder()
            .jobRun(JobRun.builder()
                .jobRunState(JobRunState.STOPPING)
                .startedOn(java.time.Instant.now())
                .executionTime(1)
                .attempt(1)
                .timeout(10)
                .build())
            .build();

        GetJobRunResponse stopped = GetJobRunResponse.builder()
            .jobRun(JobRun.builder()
                .jobRunState(JobRunState.STOPPED)
                .startedOn(java.time.Instant.now())
                .completedOn(java.time.Instant.now())
                .executionTime(2)
                .attempt(1)
                .timeout(10)
                .build())
            .build();


        when(glueClient.getJobRun(any(GetJobRunRequest.class))).thenReturn(stopping, stopped);

        StopJobRun task = spy(StopJobRun.builder()
            .region(Property.ofValue("eu-west-3"))
            .jobName(Property.ofValue("my-glue-job"))
            .jobRunId(Property.ofValue("jr-123"))
            .wait(Property.ofValue(true))
            .interval(Property.ofValue(Duration.ofMillis(10)))
            .build());

        doReturn(glueClient).when(task).glueClient(any(RunContext.class));

        Output output = task.run(runContext);

        assertNotNull(output);
        assertEquals("STOPPED", output.getState());

        verify(glueClient).batchStopJobRun(any(BatchStopJobRunRequest.class));
        verify(glueClient, atLeast(2)).getJobRun(any(GetJobRunRequest.class));
    }
}

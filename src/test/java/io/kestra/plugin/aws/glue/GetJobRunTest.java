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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@KestraTest
class GetJobRunTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void testGetLatestJobRun_mocked() throws Exception {
        var runContext = runContextFactory.of();

        GlueClient glueClient = mock(GlueClient.class);

        JobRun oldRun = JobRun.builder()
            .id("jr-old")
            .jobName("my-glue-job")
            .jobRunState(JobRunState.SUCCEEDED)
            .startedOn(java.time.Instant.parse("2024-01-01T00:00:00Z"))
            .executionTime(10)
            .attempt(1)
            .timeout(10)
            .build();

        JobRun latestRun = JobRun.builder()
            .id("jr-latest")
            .jobName("my-glue-job")
            .jobRunState(JobRunState.RUNNING)
            .startedOn(java.time.Instant.parse("2025-01-01T00:00:00Z"))
            .executionTime(1)
            .attempt(1)
            .timeout(10)
            .build();

        when(glueClient.getJobRuns(any(GetJobRunsRequest.class)))
            .thenReturn(GetJobRunsResponse.builder()
                .jobRuns(oldRun, latestRun)
                .build()
            );

        when(glueClient.getJobRun(any(GetJobRunRequest.class)))
            .thenReturn(GetJobRunResponse.builder()
                .jobRun(latestRun)
                .build()
            );

        GetJobRun task = spy(GetJobRun.builder()
            .region(Property.ofValue("eu-west-3"))
            .jobName(Property.ofValue("my-glue-job"))
            .build());

        doReturn(glueClient).when(task).glueClient(any(RunContext.class));

        Output output = task.run(runContext);

        assertNotNull(output);
        assertEquals("my-glue-job", output.getJobName());
        assertEquals("jr-latest", output.getJobRunId());
        assertEquals("RUNNING", output.getState());

        verify(glueClient).getJobRuns(any(GetJobRunsRequest.class));
        verify(glueClient).getJobRun(any(GetJobRunRequest.class));
    }
}

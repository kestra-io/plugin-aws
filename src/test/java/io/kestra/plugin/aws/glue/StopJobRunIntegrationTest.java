package io.kestra.plugin.aws.glue;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.glue.model.Output;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
@Disabled("Provide AWS credentials, an existing Glue job name, and a job run ID to stop to run this test")
class StopJobRunIntegrationTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testStopJobRunNoWait() throws Exception {
        String accessKeyId = "";
        String secretKeyId = "";
        String sessionToken = "";
        String region = "";
        String existingJobName = "";
        String existingJobRunId = "";

        StopJobRun stopJobRun = StopJobRun.builder()
            .accessKeyId(Property.ofValue(accessKeyId))
            .secretKeyId(Property.ofValue(secretKeyId))
            .sessionToken(Property.ofValue(sessionToken))
            .region(Property.ofValue(region))
            .jobName(Property.ofValue(existingJobName))
            .jobRunId(Property.ofValue(existingJobRunId))
            .wait(Property.ofValue(false))
            .build();

        Output output = stopJobRun.run(runContextFactory.of());

        assertNotNull(output.getJobRunId());
        assertEquals(existingJobName, output.getJobName());
        assertNotNull(output.getState());
    }

    @Test
    void testStopJobRunWithWait() throws Exception {
        String accessKeyId = "";
        String secretKeyId = "";
        String sessionToken = "";
        String region = "";
        String existingJobName = "";
        String existingJobRunId = "";

        StopJobRun stopJobRun = StopJobRun.builder()
            .accessKeyId(Property.ofValue(accessKeyId))
            .secretKeyId(Property.ofValue(secretKeyId))
            .sessionToken(Property.ofValue(sessionToken))
            .region(Property.ofValue(region))
            .jobName(Property.ofValue(existingJobName))
            .jobRunId(Property.ofValue(existingJobRunId))
            .wait(Property.ofValue(true))
            .interval(Property.ofValue(java.time.Duration.ofSeconds(1)))
            .build();

        Output output = stopJobRun.run(runContextFactory.of());

        assertNotNull(output.getJobRunId());
        assertEquals(existingJobName, output.getJobName());
        assertNotNull(output.getState());
        // The job should be in a stopped state
        assertTrue(output.getState().equals("STOPPED") ||
            output.getState().equals("SUCCEEDED") ||
            output.getState().equals("FAILED"));
    }
}
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
@Disabled("Provide AWS credentials and an existing Glue job name to run this test")
class StartJobRunIntegrationTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testStartJobRunNoWait() throws Exception {
        String accessKeyId = "";
        String secretKeyId = "";
        String sessionToken = "";
        String region = "";
        String existingJobName = "";
        StartJobRun startJobRun = StartJobRun.builder()
            .accessKeyId(Property.of(accessKeyId))
            .secretKeyId(Property.of(secretKeyId))
            .sessionToken(Property.of(sessionToken))
            .region(Property.of(region))
            .jobName(Property.of(existingJobName))
            .wait(Property.of(false))
            .build();

        Output output = startJobRun.run(runContextFactory.of());

        assertNotNull(output.getJobRunId());
        assertEquals(existingJobName, output.getJobName());
        assertNotNull(output.getState());
    }

}
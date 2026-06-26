package io.kestra.plugin.aws.healthlake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.healthlake.HealthLakeClient;
import software.amazon.awssdk.services.healthlake.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class StartExportJobTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenValidParams_whenStartExport_thenOutputContainsJobId() throws Exception {
        var runContext = runContextFactory.of();

        var task = StartExportJob.builder()
            .id("test-start-export")
            .type(StartExportJob.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .datastoreId(Property.ofValue("ds-abc123"))
            .outputS3Uri(Property.ofValue("s3://my-bucket/fhir/export/"))
            .dataAccessRoleArn(Property.ofValue("arn:aws:iam::123456789012:role/HealthLakeRole"))
            .build();

        var mockResponse = StartFHIRExportJobResponse.builder()
            .jobId("job-export-001")
            .jobStatus(JobStatus.SUBMITTED)
            .datastoreId("ds-abc123")
            .build();

        var mockClient = mock(HealthLakeClient.class);
        when(mockClient.startFHIRExportJob(any(StartFHIRExportJobRequest.class))).thenReturn(mockResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getJobId(), is("job-export-001"));
        assertThat(output.getJobStatus(), is("SUBMITTED"));
        assertThat(output.getDatastoreId(), is("ds-abc123"));
    }
}

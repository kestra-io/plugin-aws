package io.kestra.plugin.aws.healthlake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.healthlake.HealthLakeClient;
import software.amazon.awssdk.services.healthlake.model.*;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DescribeExportJobTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenJobId_whenDescribeExport_thenOutputContainsStatusAndS3Uri() throws Exception {
        var runContext = runContextFactory.of();

        var task = DescribeExportJob.builder()
            .id("test-describe-export")
            .type(DescribeExportJob.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .datastoreId(Property.ofValue("ds-abc123"))
            .jobId(Property.ofValue("job-export-001"))
            .build();

        var props = ExportJobProperties.builder()
            .jobId("job-export-001")
            .jobName("my-export")
            .jobStatus(JobStatus.COMPLETED)
            .datastoreId("ds-abc123")
            .submitTime(Instant.parse("2024-01-01T10:00:00Z"))
            .endTime(Instant.parse("2024-01-01T10:45:00Z"))
            .outputDataConfig(OutputDataConfig.builder()
                .s3Configuration(S3Configuration.builder()
                    .s3Uri("s3://my-bucket/fhir/export/")
                    .build())
                .build())
            .build();

        var mockResponse = DescribeFHIRExportJobResponse.builder()
            .exportJobProperties(props)
            .build();

        var mockClient = mock(HealthLakeClient.class);
        when(mockClient.describeFHIRExportJob(any(DescribeFHIRExportJobRequest.class))).thenReturn(mockResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getJobId(), is("job-export-001"));
        assertThat(output.getJobStatus(), is("COMPLETED"));
        assertThat(output.getOutputS3Uri(), is("s3://my-bucket/fhir/export/"));
        assertThat(output.getEndTime(), notNullValue());
        assertThat(output.getJobName(), is("my-export"));
        assertThat(output.getDatastoreId(), is("ds-abc123"));
    }
}

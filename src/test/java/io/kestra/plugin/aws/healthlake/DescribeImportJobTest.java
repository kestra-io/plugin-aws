package io.kestra.plugin.aws.healthlake;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.healthlake.HealthLakeClient;
import software.amazon.awssdk.services.healthlake.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DescribeImportJobTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenJobId_whenDescribe_thenOutputContainsStatus() throws Exception {
        var runContext = runContextFactory.of();

        var task = DescribeImportJob.builder()
            .id("test-describe-import")
            .type(DescribeImportJob.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .datastoreId(Property.ofValue("ds-abc123"))
            .jobId(Property.ofValue("job-import-001"))
            .build();

        var props = ImportJobProperties.builder()
            .jobId("job-import-001")
            .jobName("my-import")
            .jobStatus(JobStatus.COMPLETED)
            .datastoreId("ds-abc123")
            .submitTime(Instant.parse("2024-01-01T10:00:00Z"))
            .endTime(Instant.parse("2024-01-01T10:30:00Z"))
            .build();

        var mockResponse = DescribeFhirImportJobResponse.builder()
            .importJobProperties(props)
            .build();

        var mockClient = mock(HealthLakeClient.class);
        when(mockClient.describeFHIRImportJob(any(DescribeFhirImportJobRequest.class))).thenReturn(mockResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getJobId(), is("job-import-001"));
        assertThat(output.getJobStatus(), is("COMPLETED"));
        assertThat(output.getJobName(), is("my-import"));
        assertThat(output.getEndTime(), notNullValue());
    }
}

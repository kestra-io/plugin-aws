package io.kestra.plugin.aws.healthlake;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.healthlake.HealthLakeClient;
import software.amazon.awssdk.services.healthlake.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class TriggerTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenCompletedJob_whenEvaluate_thenReturnsExecution() throws Exception {
        var mockClient = mockImportClient(
            ImportJobProperties.builder()
                .jobId("job-import-001")
                .jobStatus(JobStatus.COMPLETED)
                .build()
        );

        var spy = spy(defaultTrigger());
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var context = TestsUtils.mockTrigger(runContextFactory, spy);
        var execution = spy.evaluate(context.getKey(), context.getValue().context());

        assertThat(execution.isPresent(), is(true));
        verify(mockClient).listFHIRImportJobs(any(ListFhirImportJobsRequest.class));
    }

    @Test
    void givenInProgressJob_whenEvaluate_thenReturnsEmpty() throws Exception {
        var mockClient = mockImportClient(
            ImportJobProperties.builder()
                .jobId("job-import-002")
                .jobStatus(JobStatus.IN_PROGRESS)
                .build()
        );

        var spy = spy(defaultTrigger());
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var context = TestsUtils.mockTrigger(runContextFactory, spy);
        var execution = spy.evaluate(context.getKey(), context.getValue().context());

        assertThat(execution.isEmpty(), is(true));
    }

    @Test
    void givenNoJobs_whenEvaluate_thenReturnsEmpty() throws Exception {
        var mockClient = mockImportClient();

        var spy = spy(defaultTrigger());
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var context = TestsUtils.mockTrigger(runContextFactory, spy);
        var execution = spy.evaluate(context.getKey(), context.getValue().context());

        assertThat(execution.isEmpty(), is(true));
    }

    private Trigger defaultTrigger() {
        return Trigger.builder()
            .id("test-trigger")
            .type(Trigger.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .datastoreId(Property.ofValue("ds-abc123"))
            .jobType(Property.ofValue(Trigger.JobType.IMPORT))
            .build();
    }

    private HealthLakeClient mockImportClient(ImportJobProperties... jobs) {
        var mockClient = mock(HealthLakeClient.class);
        when(mockClient.listFHIRImportJobs(any(ListFhirImportJobsRequest.class)))
            .thenReturn(
                ListFhirImportJobsResponse.builder()
                    .importJobPropertiesList(List.of(jobs))
                    .build()
            );
        return mockClient;
    }
}

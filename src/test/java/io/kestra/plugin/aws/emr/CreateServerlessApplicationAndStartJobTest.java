package io.kestra.plugin.aws.emr;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationResponse;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class CreateServerlessApplicationAndStartJobTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void runSpark() throws Exception {
        var runContext = runContextFactory.of();

        // Mock AWS client
        EmrServerlessClient client = mock(EmrServerlessClient.class);

        when(client.createApplication(any(CreateApplicationRequest.class)))
            .thenReturn(CreateApplicationResponse.builder()
                .applicationId("app-123")
                .build());

        when(client.startJobRun(any(StartJobRunRequest.class)))
            .thenReturn(StartJobRunResponse.builder()
                .jobRunId("job-456")
                .build());

        // Build task then spy to override client(runContext)
        var task = spy(CreateServerlessApplicationAndStartJob.builder()
            .id("create_and_run")
            .type(CreateServerlessApplicationAndStartJob.class.getName())
            .region(Property.ofValue("eu-central-1"))
            .releaseLabel(Property.ofValue("emr-7.0.0"))
            .applicationType(Property.ofValue("SPARK"))
            .executionRoleArn(Property.ofValue("arn:aws:iam::123456789012:role/EMRServerlessRole"))
            .jobName(Property.ofValue("example-job"))
            .entryPoint(Property.ofValue("s3://my-bucket/jobs/script.py"))
            .build());

        doReturn(client).when(task).client(any());

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getApplicationId(), is("app-123"));
        assertThat(output.getJobRunId(), is("job-456"));

        // Verify createApplication request
        ArgumentCaptor<CreateApplicationRequest> createCaptor =
            ArgumentCaptor.forClass(CreateApplicationRequest.class);
        verify(client, times(1)).createApplication(createCaptor.capture());

        CreateApplicationRequest createReq = createCaptor.getValue();
        assertThat(createReq.releaseLabel(), is("emr-7.0.0"));
        assertThat(createReq.type(), is("SPARK"));

        // Verify startJobRun request for Spark
        ArgumentCaptor<StartJobRunRequest> startCaptor =
            ArgumentCaptor.forClass(StartJobRunRequest.class);
        verify(client, times(1)).startJobRun(startCaptor.capture());

        StartJobRunRequest startReq = startCaptor.getValue();
        assertThat(startReq.applicationId(), is("app-123"));
        assertThat(startReq.executionRoleArn(), is("arn:aws:iam::123456789012:role/EMRServerlessRole"));
        assertThat(startReq.name(), is("example-job"));
        assertThat(startReq.jobDriver(), notNullValue());
        assertThat(startReq.jobDriver().sparkSubmit(), notNullValue());
        assertThat(startReq.jobDriver().sparkSubmit().entryPoint(),
            is("s3://my-bucket/jobs/script.py"));

        verify(client, times(1)).close();
    }

    @Test
    void runHive() throws Exception {
        var runContext = runContextFactory.of();

        EmrServerlessClient client = mock(EmrServerlessClient.class);

        when(client.createApplication(any(CreateApplicationRequest.class)))
            .thenReturn(CreateApplicationResponse.builder()
                .applicationId("app-hive")
                .build());

        when(client.startJobRun(any(StartJobRunRequest.class)))
            .thenReturn(StartJobRunResponse.builder()
                .jobRunId("job-hive")
                .build());

        var task = spy(CreateServerlessApplicationAndStartJob.builder()
            .id("create_and_run")
            .type(CreateServerlessApplicationAndStartJob.class.getName())
            .region(Property.ofValue("eu-central-1"))
            .releaseLabel(Property.ofValue("emr-7.0.0"))
            .applicationType(Property.ofValue("HIVE"))
            .executionRoleArn(Property.ofValue("arn:aws:iam::123456789012:role/EMRServerlessRole"))
            .jobName(Property.ofValue("example-job"))
            .entryPoint(Property.ofValue("SELECT 1"))
            .build());

        doReturn(client).when(task).client(any());

        var output = task.run(runContext);

        assertThat(output.getApplicationId(), is("app-hive"));
        assertThat(output.getJobRunId(), is("job-hive"));

        ArgumentCaptor<StartJobRunRequest> startCaptor =
            ArgumentCaptor.forClass(StartJobRunRequest.class);
        verify(client).startJobRun(startCaptor.capture());

        StartJobRunRequest startReq = startCaptor.getValue();
        assertThat(startReq.jobDriver().hive(), notNullValue());
        assertThat(startReq.jobDriver().hive().query(), is("SELECT 1"));

        verify(client).close();
    }

    @Test
    void runUnsupportedTypeThrows() throws Exception {
        var runContext = runContextFactory.of();

        EmrServerlessClient client = mock(EmrServerlessClient.class);
        when(client.createApplication(any(CreateApplicationRequest.class)))
            .thenReturn(CreateApplicationResponse.builder()
                .applicationId("app-zzz")
                .build());

        var task = spy(CreateServerlessApplicationAndStartJob.builder()
            .id("create_and_run")
            .type(CreateServerlessApplicationAndStartJob.class.getName())
            .region(Property.ofValue("eu-central-1"))
            .releaseLabel(Property.ofValue("emr-7.0.0"))
            .applicationType(Property.ofValue("FLINK")) // unsupported here
            .executionRoleArn(Property.ofValue("arn:aws:iam::123456789012:role/EMRServerlessRole"))
            .jobName(Property.ofValue("example-job"))
            .entryPoint(Property.ofValue("whatever"))
            .build());

        doReturn(client).when(task).client(any());

        RuntimeException ex = assertThrows(RuntimeException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("Failed to create EMR Serverless app and start job"));

        verify(client).close();
        verify(client, never()).startJobRun(any(StartJobRunRequest.class));
    }
}

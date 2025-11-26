package io.kestra.plugin.aws.emr;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class StartServerlessJobRunTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void runStartsSparkJob() throws Exception {
        var runContext = runContextFactory.of();

        // Mock AWS EMR Serverless client
        EmrServerlessClient client = mock(EmrServerlessClient.class);

        when(client.startJobRun(any(StartJobRunRequest.class)))
            .thenReturn(StartJobRunResponse.builder()
                .jobRunId("job-123")
                .build());

        // Build task and spy it to override client(runContext)
        var task = spy(StartServerlessJobRun.builder()
            .id("start_job")
            .type(StartServerlessJobRun.class.getName())
            .region(Property.ofValue("eu-central-1"))
            .applicationId(Property.ofValue("00f123abc456xyz"))
            .executionRoleArn(Property.ofValue("arn:aws:iam::123456789012:role/EMRServerlessRole"))
            .jobName(Property.ofValue("sample-spark-job"))
            .entryPoint(Property.ofValue("s3://my-bucket/scripts/spark-app.py"))
            .build());

        // Force task to use mocked client instead of real AWS client
        doReturn(client).when(task).client(any());

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getJobRunId(), is("job-123"));

        // Capture the StartJobRunRequest sent to AWS
        ArgumentCaptor<StartJobRunRequest> captor =
            ArgumentCaptor.forClass(StartJobRunRequest.class);

        // Cast is required to disambiguate AWS SDK overloads (request vs consumer)
        verify(client, times(1))
            .startJobRun((StartJobRunRequest) captor.capture());

        StartJobRunRequest req = captor.getValue();
        assertThat(req.applicationId(), is("00f123abc456xyz"));
        assertThat(req.executionRoleArn(), is("arn:aws:iam::123456789012:role/EMRServerlessRole"));
        assertThat(req.name(), is("sample-spark-job"));

        // Validate Spark submit driver and entry point
        assertThat(req.jobDriver(), notNullValue());
        assertThat(req.jobDriver().sparkSubmit(), notNullValue());
        assertThat(req.jobDriver().sparkSubmit().entryPoint(),
            is("s3://my-bucket/scripts/spark-app.py"));

        // Verify that the client is closed (try-with-resources)
        verify(client, times(1)).close();
    }

    @Test
    void runPropagatesAwsFailureAsRuntimeException() throws Exception {
        var runContext = runContextFactory.of();

        // Mock AWS EMR Serverless client that fails
        EmrServerlessClient client = mock(EmrServerlessClient.class);
        when(client.startJobRun(any(StartJobRunRequest.class)))
            .thenThrow(RuntimeException.class);

        var task = spy(StartServerlessJobRun.builder()
            .id("start_job")
            .type(StartServerlessJobRun.class.getName())
            .region(Property.ofValue("eu-central-1"))
            .applicationId(Property.ofValue("00f123abc456xyz"))
            .executionRoleArn(Property.ofValue("arn:aws:iam::123456789012:role/EMRServerlessRole"))
            .jobName(Property.ofValue("sample-spark-job"))
            .entryPoint(Property.ofValue("s3://my-bucket/scripts/spark-app.py"))
            .build());

        doReturn(client).when(task).client(any());

        RuntimeException ex = org.junit.jupiter.api.Assertions.assertThrows(
            RuntimeException.class,
            () -> task.run(runContext)
        );

        assertThat(ex.getMessage(), containsString("Failed to start EMR Serverless job"));

        // Ensure the AWS call was attempted
        verify(client, times(1))
            .startJobRun((StartJobRunRequest) any(StartJobRunRequest.class));

        verify(client).close();
    }
}

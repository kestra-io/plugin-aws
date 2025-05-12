package io.kestra.plugin.aws.glue;

import software.amazon.awssdk.services.glue.model.GetJobRunRequest;

public class GlueService {

    /**
     * Creates a GetJobRunRequest with the given job name and run ID
     */
    public static GetJobRunRequest createGetJobRunRequest(String jobName, String runId) {
        return GetJobRunRequest.builder()
            .jobName(jobName)
            .runId(runId)
            .build();
    }
}

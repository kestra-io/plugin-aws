package io.kestra.plugin.aws.runner;

import com.google.common.annotations.Beta;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.script.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import reactor.core.CoreSubscriber;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.batch.BatchClient;
import software.amazon.awssdk.services.batch.BatchClientBuilder;
import software.amazon.awssdk.services.batch.model.*;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

@Introspected
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Beta
@Schema(title = "AWS Batch script runner", description = """
    Run a script in a container on an AWS Batch Compute Environment.
    This runner will wait for the task to succeed or fail up to a max `waitUntilCompletion` duration.
    It will return with an exit code according to the following mapping:
    - SUCCEEDED: 0
    - FAILED: 1
    - RUNNING: 2
    - RUNNABLE: 3
    - PENDING: 4
    - STARTING: 5
    - SUBMITTED: 6
    - OTHER: -1""")
public class AwsBatchScriptRunner extends ScriptRunner implements AbstractConnectionInterface {
    private static final Map<JobStatus, Integer> exitCodeByStatus = Map.of(
        JobStatus.FAILED, 1,
        JobStatus.SUBMITTED, 6,
        JobStatus.PENDING, 4,
        JobStatus.RUNNABLE, 3,
        JobStatus.STARTING, 5,
        JobStatus.RUNNING, 2,
        JobStatus.UNKNOWN_TO_SDK_VERSION, -1
    );

    @NotNull
    @PluginProperty(dynamic = true)
    private String region;
    @PluginProperty(dynamic = true)
    private String endpointOverride;

    // Configuration for StaticCredentialsProvider
    @PluginProperty(dynamic = true)
    private String accessKeyId;
    @PluginProperty(dynamic = true)
    private String secretKeyId;
    @PluginProperty(dynamic = true)
    private String sessionToken;

    // Configuration for AWS STS AssumeRole
    @PluginProperty(dynamic = true)
    private String stsRoleArn;
    @PluginProperty(dynamic = true)
    private String stsRoleExternalId;
    @PluginProperty(dynamic = true)
    private String stsRoleSessionName;
    @PluginProperty(dynamic = true)
    private String stsEndpointOverride;
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Duration stsRoleSessionDuration = AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION;

    @Schema(
        title = "Compute environment on which to run the job."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String computeEnvironmentArn;

    @Schema(
        title = "Job queue to use to submit jobs (ARN). If not specified, will create one."
    )
    @PluginProperty(dynamic = true)
    private String jobQueueArn;

    @Schema(
        title = "S3 Bucket to use to upload (inputFiles) and download (outputFiles) files. Can be left empty if not using such features."
    )
    @PluginProperty(dynamic = true)
    private String s3Bucket;

    @Schema(
        title = "Execution role to use to run the job. Mandatory if the compute environment is a Fargate one."
    )
    @PluginProperty(dynamic = true)
    private String executionRoleArn;

    @Schema(
        title = "Container custom resources requests. If using a Fargate compute environments, resources requests must match this table: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html"
    )
    @PluginProperty
    @Builder.Default
    private Resources resources = Resources.builder()
        .request(
            Resource.builder()
                .memory("2048")
                .cpu("1")
                .build()
        ).build();

    @Schema(
        title = "The maximum duration to wait for the pod completion. AWS Batch will automatically timeout the job upon reaching such duration and the task will be failed."
    )
    @Builder.Default
    private final Duration waitUntilCompletion = Duration.ofHours(1);

    @Override
    public RunnerResult run(RunContext runContext, ScriptCommands commands, List<String> filesToUpload, List<String> filesToDownload) throws Exception {
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        Logger logger = runContext.logger();
        AbstractLogConsumer logConsumer = commands.getLogConsumer();

        String renderedRegion = runContext.render(this.region);
        BatchClientBuilder batchClientBuilder = BatchClient.builder()
            .credentialsProvider(AbstractConnectionInterface.credentialsProvider(this.awsClientConfig(runContext)))
            .region(Region.of(renderedRegion))
            // Use the httpClientBuilder to delegate the lifecycle management of the HTTP client to the AWS SDK
            .httpClientBuilder(serviceDefaults -> ApacheHttpClient.builder().build());

        if (this.endpointOverride != null) {
            batchClientBuilder.endpointOverride(URI.create(runContext.render(this.endpointOverride)));
        }

        BatchClient client = batchClientBuilder
            .build();

        String jobName = IdUtils.create();
        ContainerProperties.Builder containerPropsBuilder = ContainerProperties.builder()
            .image(runContext.render(commands.getContainerImage()))
            .logConfiguration(
                LogConfiguration.builder()
                    .logDriver(LogDriver.AWSLOGS)
                    .options(Map.of("awslogs-stream-prefix", jobName))
                    .build()
            );

        if (this.resources != null && this.resources.getRequest() != null) {
            containerPropsBuilder
                .resourceRequirements(
                    ResourceRequirement.builder()
                        .type(ResourceType.MEMORY)
                        .value(this.resources.getRequest().getMemory())
                        .build(),
                    ResourceRequirement.builder()
                        .type(ResourceType.VCPU)
                        .value(this.resources.getRequest().getCpu())
                        .build()
                );
        }

        if (this.executionRoleArn != null) {
            containerPropsBuilder.executionRoleArn(runContext.render(this.executionRoleArn));
        }

        if (commands.getEnv() != null) {
            containerPropsBuilder
                .environment(
                    commands.getEnv().entrySet().stream()
                        .map(e -> KeyValuePair.builder().name(e.getKey()).value(e.getValue()).build())
                        .toArray(KeyValuePair[]::new)
                );
        }

        Map<String, Object> additionalVars = Optional.ofNullable(runContext.render(s3Bucket))
            .map(bucket -> Map.<String, Object>of(
                "workingDir", "s3:" + bucket + "/"+IdUtils.create(),
                "outputDir", "s3:" + bucket + "/"+IdUtils.create()
            ))
            .orElse(Collections.emptyMap());
        List<String> command = ScriptService.uploadInputFiles(runContext, runContext.render(commands.getCommands(), additionalVars));

        containerPropsBuilder.command(command);

        ComputeEnvironmentDetail computeEnvironmentDetail = client.describeComputeEnvironments(
            DescribeComputeEnvironmentsRequest.builder()
                .computeEnvironments(computeEnvironmentArn)
                .build()
        ).computeEnvironments().stream()
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Compute environment not found: " + computeEnvironmentArn));

        PlatformCapability platformCapability = switch (computeEnvironmentDetail.computeResources().type()) {
            case FARGATE:
            case FARGATE_SPOT:
                yield PlatformCapability.FARGATE;
            case EC2:
            case SPOT:
                yield PlatformCapability.EC2;
            default:
                yield null;
        };

        if (platformCapability == PlatformCapability.FARGATE) {
            containerPropsBuilder.networkConfiguration(
                NetworkConfiguration.builder()
                    .assignPublicIp(AssignPublicIp.ENABLED)
                    .build()
            );
        }


        logger.debug("Registering job definition");
        RegisterJobDefinitionResponse registerJobDefinitionResponse = client.registerJobDefinition(
            RegisterJobDefinitionRequest.builder()
                .jobDefinitionName(IdUtils.create())
                .type(JobDefinitionType.CONTAINER)
                .platformCapabilities(platformCapability)
                .containerProperties(containerPropsBuilder.build())
                .build()
        );
        String jobDefArn = registerJobDefinitionResponse.jobDefinitionArn();
        logger.debug("Job definition successfully registered: {}", jobDefArn);

        String jobQueue = runContext.render(this.jobQueueArn);
        if (jobQueue == null) {
            logger.debug("Job queue not specified, creating a one-use job queue");
            CreateJobQueueResponse jobQueueResponse = client.createJobQueue(
                CreateJobQueueRequest.builder()
                    .jobQueueName(IdUtils.create())
                    .priority(10)
                    .computeEnvironmentOrder(
                        ComputeEnvironmentOrder.builder()
                            .order(1)
                            .computeEnvironment(runContext.render(this.computeEnvironmentArn))
                            .build()
                    )
                    .build()
            );

            waitForQueueUpdate(client, jobQueueResponse);

            jobQueue = jobQueueResponse.jobQueueArn();
            logger.debug("Job queue created: {}", jobQueue);
        }

        CloudWatchLogsAsyncClient cloudWatchLogsAsyncClient =
            CloudWatchLogsAsyncClient.builder()
                .credentialsProvider(AbstractConnectionInterface.credentialsProvider(this.awsClientConfig(runContext)))
                .build();
        try {
            String logGroupArn = cloudWatchLogsAsyncClient.describeLogGroups(
                    DescribeLogGroupsRequest.builder()
                        .logGroupNamePrefix("/aws/batch/job")
                        .build()
                ).get().logGroups().stream()
                .filter(logGroup -> logGroup.arn().contains(renderedRegion))
                .findFirst()
                .map(LogGroup::arn)
                .map(arn -> arn.endsWith("*") ? arn.substring(0, arn.length() - 1) : arn)
                .orElse(null);

            StartLiveTailRequest request = StartLiveTailRequest.builder()
                .logGroupIdentifiers(logGroupArn)
                .logStreamNamePrefixes(jobName)
                .build();

            cloudWatchLogsAsyncClient.startLiveTail(request, getStartLiveTailResponseStreamHandler(logger, logConsumer));

            logger.debug("Submitting job to queue");
            SubmitJobResponse submitJobResponse = client.submitJob(
                SubmitJobRequest.builder()
                    .jobName(jobName)
                    .jobDefinition(jobDefArn)
                    .jobQueue(jobQueue)
                    .timeout(
                        JobTimeout.builder()
                            .attemptDurationSeconds((int) this.waitUntilCompletion.toSeconds())
                            .build()
                    )
                    .build()
            );
            logger.debug("Job submitted: {}", submitJobResponse.jobId());

            final AtomicReference<DescribeJobsResponse> describeJobsResponse = new AtomicReference<>();
            try {
                Await.until(() -> {
                    describeJobsResponse.set(client.describeJobs(
                        DescribeJobsRequest.builder()
                            .jobs(submitJobResponse.jobId())
                            .build()
                    ));

                    JobStatus status = describeJobsResponse.get().jobs().get(0).status();

                    if (status == JobStatus.FAILED) {
                        throw new RuntimeException();
                    }

                    return status == JobStatus.SUCCEEDED;
                }, Duration.ofMillis(500), this.waitUntilCompletion);
            } catch (TimeoutException | RuntimeException e) {
                Integer exitCode = exitCodeByStatus.get(describeJobsResponse.get().jobs().get(0).status());
                return new RunnerResult(exitCode, commands.getLogConsumer());
            }
        } finally {
            cleanup(client, jobQueue, jobDefArn);
            // Manual close after cleanup to make sure we get all remaining logs
            cloudWatchLogsAsyncClient.close();
        }

        return new RunnerResult(0, commands.getLogConsumer());
    }

    private static StartLiveTailResponseHandler getStartLiveTailResponseStreamHandler(Logger logger, AbstractLogConsumer logConsumer) {
        return StartLiveTailResponseHandler.builder()
            .onError(throwable -> {
                CloudWatchLogsException e = (CloudWatchLogsException) throwable.getCause();
                logger.error(e.awsErrorDetails().errorMessage());
            })
            .subscriber(() -> new CoreSubscriber<>() {
                @Override
                public void onSubscribe(@NonNull Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(StartLiveTailResponseStream event) {
                    if (event instanceof LiveTailSessionStart) {
                        // do nothing
                    } else if (event instanceof LiveTailSessionUpdate) {
                        LiveTailSessionUpdate sessionUpdate = (LiveTailSessionUpdate) event;
                        List<LiveTailSessionLogEvent> logEvents = sessionUpdate.sessionResults();
                        logEvents.forEach(e -> logConsumer.accept("[JOB LOG] " + e.message(), false));
                    } else {
                        throw CloudWatchLogsException.builder().message("Unknown event type").build();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.error(throwable.getMessage());
                }

                @Override
                public void onComplete() {
                    // no-op
                }
            })
            .build();
    }

    private static void waitForQueueUpdate(BatchClient client, CreateJobQueueResponse jobQueueResponse) throws TimeoutException {
        Await.until(() -> {
            DescribeJobQueuesResponse describeJobQueuesResponse = client.describeJobQueues(
                DescribeJobQueuesRequest.builder()
                    .jobQueues(jobQueueResponse.jobQueueArn())
                    .build()
            );

            return describeJobQueuesResponse.jobQueues().get(0).status() == JQStatus.VALID;
        }, Duration.ofMillis(500), Duration.ofMinutes(1));
    }

    private void cleanup(BatchClient client, String jobQueue, String jobDefArn) throws TimeoutException {
        // We created one and should delete it at the end
        if (this.jobQueueArn == null) {
            client.updateJobQueue(
                UpdateJobQueueRequest.builder()
                    .jobQueue(jobQueue)
                    .state(JQState.DISABLED)
                    .build()
            );

            waitForQueueUpdate(client, CreateJobQueueResponse.builder().jobQueueArn(jobQueue).build());
            client.deleteJobQueue(
                DeleteJobQueueRequest.builder()
                    .jobQueue(jobQueue)
                    .build()
            );
        }

        client.deregisterJobDefinition(
            DeregisterJobDefinitionRequest.builder()
                .jobDefinition(jobDefArn)
                .build()
        );
    }

    @Getter
    @Builder
    public static class Resources {
        private Resource request;
    }

    @Getter
    @Builder
    public static class Resource {
        private String memory;
        private String cpu;
    }
}
package io.kestra.plugin.aws.runner;

import com.google.common.collect.Iterables;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.ListUtils;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.kestra.plugin.aws.ConnectionUtils;
import io.kestra.plugin.aws.s3.AbstractS3;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
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
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Task runner that executes a task inside a job in AWS Batch.",
    description = """
This task runner only supports ECS Fargate or ECS EC2 as compute environment. 
For EKS, use the [Kubernetes Task Runner](https://kestra.io/plugins/plugin-kubernetes/task-runners/runner/io.kestra.plugin.kubernetes.runner.kubernetes). 

Make sure to set the `containerImage` property because this runner runs the task in a container.

To access the task's working directory, use the `{{ workingDir }}` Pebble expression or the `WORKING_DIR` environment variable. 
This directory will contain all input files and namespace files (if enabled).

To generate output files you can either use the `outputFiles` task property and create a file with the same name in the task's working directory, or create any file in the output directory which can be accessed using the `{{ outputDir }}` Pebble expression or the `OUTPUT_DIR` environment variable.

To use `inputFiles`, `outputFiles` or `namespaceFiles` properties, make sure to set the `bucket` property. The bucket serves as an intermediary storage layer for the task runner. Input and namespace files will be uploaded to the cloud storage bucket before the task run starts. Similarly, the task runner will store `outputFiles` in this bucket during the task run. In the end, the task runner will make those files available for download and preview from the UI by sending them to internal storage.

To make it easier to track where all files are stored, the task runner will generate a folder for each task run. You can access that folder using the `{{bucketPath}}` Pebble expression or the `BUCKET_PATH` environment variable.

Note that this task runner executes the task in the root directory. You need to use the `{{ workingDir }}` Pebble expression or the `WORKING_DIR` environment variable to access files in the task's working directory.

This task runner will return with an exit code according to the following mapping:
- SUCCEEDED: 0
- FAILED: 1
- RUNNING: 2
- RUNNABLE: 3
- PENDING: 4
- STARTING: 5
- SUBMITTED: 6
- OTHER: -1

When the Kestra Worker running this task is terminated, the batch job will still run until completion.
To avoid zombie containers in ECS, you can set the `timeout` property on the task and kestra will terminate the batch job if the task is not completed within the specified duration."""
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a Shell command in a container on ECS Fargate.",
            code = """
                id: run_container
                namespace: dev
    
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    taskRunner:
                      type: io.kestra.plugin.aws.runner.Batch
                      accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                      secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                      region: "{{ vars.region }}"
                      computeEnvironmentArn: "{{ vars.computeEnvironmentArn }}"
                    commands:
                    - echo "Hello World\"""",
            full = true
        ),
        @Example(
            title = "Pass input files to the task, execute a Shell command, then retrieve the output files.",
            code = """
                id: container_with_input_files
                namespace: myteam
                
                inputs:
                  - id: file
                    type: FILE
                
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    inputFiles:
                      data.txt: "{{ inputs.file }}"
                    outputFiles:
                      - out.txt
                    containerImage: centos
                    taskRunner:
                      type: io.kestra.plugin.aws.runner.Batch
                      accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                      secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                      region: "{{ vars.region }}"
                      computeEnvironmentArn: "{{ vars.computeEnvironmentArn }}"
                      bucket: "{{ vars.bucket }}"
                    commands:
                    - cp {{ workingDir }}/data.txt {{ workingDir }}/out.txt""",
            full = true
        )
    }
)
public class Batch extends TaskRunner implements AbstractS3, AbstractConnectionInterface, RemoteRunnerInterface {
    private static final Map<JobStatus, Integer> exitCodeByStatus = Map.of(
        JobStatus.FAILED, 1,
        JobStatus.RUNNING, 2,
        JobStatus.RUNNABLE, 3,
        JobStatus.PENDING, 4,
        JobStatus.STARTING, 5,
        JobStatus.SUBMITTED, 6,
        JobStatus.UNKNOWN_TO_SDK_VERSION, -1
    );

    @NotNull
    private String region;
    private String endpointOverride;

    // Configuration for StaticCredentialsProvider
    private String accessKeyId;
    private String secretKeyId;
    private String sessionToken;

    // Configuration for AWS STS AssumeRole
    private String stsRoleArn;
    private String stsRoleExternalId;
    private String stsRoleSessionName;
    private String stsEndpointOverride;
    @Builder.Default
    private Duration stsRoleSessionDuration = AbstractConnectionInterface.AWS_MIN_STS_ROLE_SESSION_DURATION;

    @Schema(
        title = "Compute environment in which to run the job."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String computeEnvironmentArn;

    @Schema(
        title = "Job queue to use to submit jobs (ARN). If not specified, the task runner will create a job queue — keep in mind that this can lead to a longer execution."
    )
    @PluginProperty(dynamic = true)
    private String jobQueueArn;

    @Schema(
        title = "S3 Bucket to upload (`inputFiles` and `namespaceFiles`) and download (`outputFiles`) files.",
        description = "It's mandatory to provide a bucket if you want to use such properties."
    )
    @PluginProperty(dynamic = true)
    private String bucket;

    @Schema(
        title = "Execution role for the AWS Batch job.",
        description = "Mandatory if the compute environment is ECS Fargate. See the [AWS documentation](https://docs.aws.amazon.com/batch/latest/userguide/execution-IAM-role.html) for more details."
    )
    @PluginProperty(dynamic = true)
    private String executionRoleArn;

    @Schema(
        title = "Task role to use within the container.",
        description = "Needed if you want to authenticate with AWS CLI within your container."
    )
    @PluginProperty(dynamic = true)
    private String taskRoleArn;

    @Schema(
        title = "Custom resources for the ECS Fargate container.",
        description = "See the [AWS documentation](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html) for more details."
    )
    @PluginProperty
    @NotNull
    @Builder.Default
    private Resources resources = Resources.builder()
        .request(
            Resource.builder()
                .memory("2048")
                .cpu("1")
                .build()
        ).build();

    @Schema(
        title = "The maximum duration to wait for the job completion unless the task `timeout` property is set which will take precedence over this property.",
        description = "AWS Batch will automatically timeout the job upon reaching that duration and the task will be marked as failed."
    )
    @Builder.Default
    private Duration waitUntilCompletion = Duration.ofHours(1);

    @Schema(
        title = "Determines how often Kestra should poll the container for completion. By default, the task runner checks every 5 seconds whether the job is completed. You can set this to a lower value (e.g. `PT0.1S` = every 100 milliseconds) for quick jobs and to a lower threshold (e.g. `PT1M` = every minute) for long-running jobs. Setting this property to a lower value will reduce the number of API calls Kestra makes to the remote service — keep that in mind in case you see API rate limit errors."
    )
    @Builder.Default
    @PluginProperty
    private final Duration completionCheckInterval = Duration.ofSeconds(5);

    @Override
    public RunnerResult run(RunContext runContext, TaskCommands taskCommands, List<String> filesToUpload, List<String> filesToDownload) throws Exception {
        boolean hasS3Bucket = this.bucket != null;

        String renderedBucket = runContext.render(bucket);

        boolean hasFilesToUpload = !ListUtils.isEmpty(filesToUpload);
        if (hasFilesToUpload && !hasS3Bucket) {
            throw new IllegalArgumentException("You must provide an S3 bucket in order to use `inputFiles` or `namespaceFiles`");
        }
        boolean hasFilesToDownload = !ListUtils.isEmpty(filesToDownload);
        boolean outputDirectoryEnabled = taskCommands.outputDirectoryEnabled();
        if ((hasFilesToDownload || outputDirectoryEnabled) && !hasS3Bucket) {
            throw new IllegalArgumentException("You must provide an S3 bucket in order to use `outputFiles` or `{{ outputDir }}`");
        }

        Logger logger = runContext.logger();
        AbstractLogConsumer logConsumer = taskCommands.getLogConsumer();

        String renderedRegion = runContext.render(this.region);
        Region regionObject = Region.of(renderedRegion);
        BatchClient client = newBatchClient(runContext, regionObject);

        String jobName = ScriptService.jobName(runContext);

        String renderedComputeEnvironmentArn = runContext.render(this.computeEnvironmentArn);
        ComputeEnvironmentDetail computeEnvironmentDetail = client.describeComputeEnvironments(
                DescribeComputeEnvironmentsRequest.builder()
                    .computeEnvironments(renderedComputeEnvironmentArn)
                    .build()
            ).computeEnvironments().stream()
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Compute environment not found: " + renderedComputeEnvironmentArn));

        String kestraVolume = "kestra";
        if (computeEnvironmentDetail.containerOrchestrationType() != OrchestrationType.ECS) {
            throw new IllegalArgumentException("Only ECS compute environments are supported");
        }

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

        String jobDefinitionName = IdUtils.create();
        RegisterJobDefinitionRequest.Builder jobDefBuilder = RegisterJobDefinitionRequest.builder()
            .jobDefinitionName(jobDefinitionName)
            .type(JobDefinitionType.CONTAINER)
            .tags(ScriptService.labels(runContext, "kestra-", true, true))
            .platformCapabilities(platformCapability);
        Path batchWorkingDirectory = (Path) this.additionalVars(runContext, taskCommands).get(ScriptService.VAR_WORKING_DIR);

        if (hasFilesToUpload) {
            try (S3TransferManager transferManager = transferManager(runContext)) {
                filesToUpload.stream().map(relativePath ->
                        UploadFileRequest.builder()
                            .putObjectRequest(
                                PutObjectRequest
                                    .builder()
                                    .bucket(renderedBucket)
                                    // Use path to eventually deduplicate leading '/'
                                    .key((batchWorkingDirectory + Path.of("/" + relativePath).toString()).substring(1))
                                    .build()
                            )
                            .source(runContext.resolve(Path.of(relativePath)))
                            .build()
                    ).map(transferManager::uploadFile)
                    .map(FileUpload::completionFuture)
                    .forEach(throwConsumer(CompletableFuture::get));
            }
        }

        EcsTaskProperties.Builder taskPropertiesBuilder = EcsTaskProperties.builder()
            .volumes(
                Volume.builder()
                    .name(kestraVolume)
                    .build()
            );

        if (platformCapability == PlatformCapability.FARGATE) {
            taskPropertiesBuilder
                .networkConfiguration(
                    NetworkConfiguration.builder()
                        .assignPublicIp(AssignPublicIp.ENABLED)
                        .build()
                );
        }

        if (this.executionRoleArn != null) {
            taskPropertiesBuilder.executionRoleArn(runContext.render(this.executionRoleArn));
        }

        if (this.taskRoleArn != null) {
            taskPropertiesBuilder.taskRoleArn(runContext.render(this.taskRoleArn));
        }

        List<TaskContainerProperties> containers = new ArrayList<>();
        String inputFilesContainerName = "inputFiles";
        String mainContainerName = "main";
        String outputFilesContainerName = "outputFiles";

        int baseSideContainerMemory = 128;
        float baseSideContainerCpu = 0.1f;
        Map<String, Object> additionalVars = this.additionalVars(runContext, taskCommands);
        Object s3WorkingDir = additionalVars.get(ScriptService.VAR_BUCKET_PATH);
        Path batchOutputDirectory = (Path) additionalVars.get(ScriptService.VAR_OUTPUT_DIR);
        MountPoint volumeMount = MountPoint.builder()
            .containerPath(batchWorkingDirectory.toString())
            .sourceVolume(kestraVolume)
            .build();

        if (hasFilesToUpload || outputDirectoryEnabled) {
            Stream<String> commands = ListUtils.emptyOnNull(filesToUpload).stream()
                    .map(relativePath -> "aws s3 cp " + s3WorkingDir + Path.of("/" + relativePath) + " " + batchWorkingDirectory + Path.of("/" + relativePath));
            if (outputDirectoryEnabled) {
                commands = Stream.concat(commands, Stream.of("mkdir " + batchOutputDirectory));
            }
            containers.add(
                withResources(
                    TaskContainerProperties.builder()
                        .image("ghcr.io/kestra-io/awsbatch:latest")
                        .mountPoints(volumeMount)
                        .essential(false)
                        .command(ScriptService.scriptCommands(
                            List.of("/bin/sh", "-c"),
                            null,
                            commands.toList()
                        ))
                        .name(inputFilesContainerName),
                    baseSideContainerMemory,
                    baseSideContainerCpu).build()
            );
        }

        int sideContainersAmount = 0;
        if (outputDirectoryEnabled) {
            sideContainersAmount = 2;
        } else {
            if (hasFilesToUpload) {
                sideContainersAmount++;
            }

            if (hasFilesToDownload) {
                sideContainersAmount++;
            }
        }
        int sideContainersMemoryAllocations = baseSideContainerMemory * sideContainersAmount;
        float sideContainersCpuAllocations = baseSideContainerCpu * sideContainersAmount;

        boolean needsOutputFilesContainer = hasFilesToDownload || outputDirectoryEnabled;
        TaskContainerProperties.Builder mainContainerBuilder = withResources(
            TaskContainerProperties.builder()
                .image(taskCommands.getContainerImage())
                .command(taskCommands.getCommands())
                .name(mainContainerName)
                .logConfiguration(
                    LogConfiguration.builder()
                        .logDriver(LogDriver.AWSLOGS)
                        .options(Map.of("awslogs-stream-prefix", jobName))
                        .build()
                )
                .environment(
                    this.env(runContext, taskCommands).entrySet().stream()
                        .map(e -> KeyValuePair.builder().name(e.getKey()).value(e.getValue()).build())
                        .toArray(KeyValuePair[]::new)
                )
                .essential(!needsOutputFilesContainer),
            Integer.parseInt(resources.getRequest().getMemory()) - sideContainersMemoryAllocations,
            Float.parseFloat(resources.getRequest().getCpu()) - sideContainersCpuAllocations
        );

        if (hasFilesToUpload || needsOutputFilesContainer) {
            mainContainerBuilder.dependsOn(TaskContainerDependency.builder().containerName(inputFilesContainerName).condition("SUCCESS").build());
            mainContainerBuilder.mountPoints(volumeMount);
        }
        containers.add(mainContainerBuilder.build());

        if (needsOutputFilesContainer) {
            Stream<String> commands = filesToDownload.stream()
                    .map(relativePath -> "aws s3 cp " + batchWorkingDirectory + "/" + relativePath + " " + s3WorkingDir + Path.of("/" + relativePath));
            if (outputDirectoryEnabled) {
                commands = Stream.concat(commands, Stream.of("aws s3 cp " + batchOutputDirectory + "/ " + s3WorkingDir + "/" + batchWorkingDirectory.relativize(batchOutputDirectory) + "/ --recursive"));
            }
            containers.add(
                withResources(
                    TaskContainerProperties.builder()
                        .image("ghcr.io/kestra-io/awsbatch:latest")
                        .mountPoints(volumeMount)
                        .command(ScriptService.scriptCommands(
                            List.of("/bin/sh", "-c"),
                            null,
                            commands.toList()
                        ))
                        .dependsOn(TaskContainerDependency.builder().containerName(mainContainerName).condition("SUCCESS").build())
                        .name(outputFilesContainerName),
                    baseSideContainerMemory,
                    baseSideContainerCpu).build()
            );
        }

        taskPropertiesBuilder.containers(containers);

        jobDefBuilder.ecsProperties(
            EcsProperties.builder()
                .taskProperties(taskPropertiesBuilder.build())
                .build()
        );

        logger.debug("Registering job definition");
        RegisterJobDefinitionResponse registerJobDefinitionResponse = client.registerJobDefinition(jobDefBuilder.build());
        logger.debug("Job definition successfully registered: {}", jobDefinitionName);

        String jobQueue = runContext.render(this.jobQueueArn);
        if (jobQueue == null) {
            logger.debug("Job queue not specified, creating a job queue for the current job");
            CreateJobQueueResponse jobQueueResponse = client.createJobQueue(
                CreateJobQueueRequest.builder()
                    .jobQueueName(IdUtils.create())
                    .priority(10)
                    .computeEnvironmentOrder(
                        ComputeEnvironmentOrder.builder()
                            .order(1)
                            .computeEnvironment(renderedComputeEnvironmentArn)
                            .build()
                    )
                    .build()
            );

            waitForQueueUpdate(client, jobQueueResponse);

            jobQueue = jobQueueResponse.jobQueueArn();

            logger.debug("Job queue created: {}", jobQueue);
        }

        String jobDefArn = registerJobDefinitionResponse.jobDefinitionArn();

        CloudWatchLogsAsyncClient cloudWatchLogsAsyncClient =
            CloudWatchLogsAsyncClient.builder()
                .credentialsProvider(ConnectionUtils.credentialsProvider(this.awsClientConfig(runContext)))
                .region(regionObject)
                .build();
        try {
            String logGroupName = "/aws/batch/job";
            String logGroupArn = getLogGroupArn(cloudWatchLogsAsyncClient, logGroupName, renderedRegion);

            if (logGroupArn == null) {
                cloudWatchLogsAsyncClient.createLogGroup(
                    CreateLogGroupRequest.builder()
                        .logGroupName(logGroupName)
                        .build()
                ).get();

                logGroupArn = getLogGroupArn(cloudWatchLogsAsyncClient, logGroupName, renderedRegion);
            }

            StartLiveTailRequest request = StartLiveTailRequest.builder()
                .logGroupIdentifiers(logGroupArn)
                .logStreamNamePrefixes(jobName)
                .build();

            cloudWatchLogsAsyncClient.startLiveTail(request, getStartLiveTailResponseStreamHandler(logConsumer));

            logger.debug("Submitting job to the job queue");
            Duration waitDuration = Optional.ofNullable(taskCommands.getTimeout()).orElse(this.waitUntilCompletion);
            SubmitJobResponse submitJobResponse = client.submitJob(
                SubmitJobRequest.builder()
                    .jobName(jobName)
                    .jobDefinition(jobDefArn)
                    .jobQueue(jobQueue)
                    .timeout(
                        JobTimeout.builder()
                            .attemptDurationSeconds((int) waitDuration.toSeconds())
                            .build()
                    )
                    .build()
            );
            onKill(() -> safelyKillJob(runContext, regionObject, submitJobResponse.jobId()));
            logger.debug("Job submitted: {}", submitJobResponse.jobName());

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
                }, completionCheckInterval, waitDuration);
            } catch (TimeoutException | RuntimeException e) {
                JobStatus status = describeJobsResponse.get().jobs().get(0).status();
                Integer exitCode = exitCodeByStatus.get(status);
                logConsumer.accept("AWS Batch job finished with status " + status.name() + ". Please check the job with name " + jobName + " for more details.", true);
                throw new TaskException(exitCode, logConsumer.getStdOutCount(), logConsumer.getStdErrCount());
            }

            if (hasFilesToDownload || outputDirectoryEnabled) {
                try (S3TransferManager transferManager = transferManager(runContext)) {
                    filesToDownload.stream().map(relativePath -> transferManager.downloadFile(
                            DownloadFileRequest.builder()
                                .getObjectRequest(GetObjectRequest.builder()
                                    .bucket(renderedBucket)
                                    .key((batchWorkingDirectory + "/" + relativePath).substring(1))
                                    .build())
                                .destination(taskCommands.getWorkingDirectory().resolve(Path.of(relativePath.startsWith("/") ? relativePath.substring(1) : relativePath)))
                                .build()
                        )).map(FileDownload::completionFuture)
                        .forEach(throwConsumer(CompletableFuture::get));

                    if (outputDirectoryEnabled) {
                        transferManager.downloadDirectory(DownloadDirectoryRequest.builder()
                                .bucket(renderedBucket)
                                .destination(taskCommands.getOutputDirectory())
                                .listObjectsV2RequestTransformer(builder -> builder
                                    .prefix(batchOutputDirectory.toString().substring(1))
                                )
                                .build())
                            .completionFuture()
                            .get();
                    }
                }
            }
        } finally {
            cleanupBatchResources(client, jobQueue, jobDefArn);
            // Manual close after cleanup to make sure we get all remaining logs
            cloudWatchLogsAsyncClient.close();
            if (hasS3Bucket) {
                cleanupS3Resources(runContext, batchWorkingDirectory);
            }
        }

        return new RunnerResult(0, logConsumer);
    }

    private BatchClient newBatchClient(RunContext runContext, Region regionObject) throws IllegalVariableEvaluationException {
        BatchClientBuilder batchClientBuilder = BatchClient.builder()
            .credentialsProvider(ConnectionUtils.credentialsProvider(this.awsClientConfig(runContext)))
            .region(regionObject)
            // Use the httpClientBuilder to delegate the lifecycle management of the HTTP client to the AWS SDK
            .httpClientBuilder(serviceDefaults -> ApacheHttpClient.builder().build());

        if (this.endpointOverride != null) {
            batchClientBuilder.endpointOverride(URI.create(runContext.render(this.endpointOverride)));
        }

        return batchClientBuilder.build();
    }

    @Override
    public Map<String, Object> runnerAdditionalVars(RunContext runContext, TaskCommands taskCommands) throws IllegalVariableEvaluationException {
        Map<String, Object> additionalVars = new HashMap<>();
        Path batchWorkingDirectory = Path.of("/" + IdUtils.create());
        additionalVars.put(ScriptService.VAR_WORKING_DIR, batchWorkingDirectory);

        if (this.bucket != null) {
            additionalVars.put(ScriptService.VAR_BUCKET_PATH, "s3://" + runContext.render(this.bucket) + batchWorkingDirectory);
        }

        if (taskCommands.outputDirectoryEnabled()) {
            Path batchOutputDirectory = batchWorkingDirectory.resolve(IdUtils.create());
            additionalVars.put(ScriptService.VAR_OUTPUT_DIR, batchOutputDirectory);
        }

        return additionalVars;
    }

    @Nullable
    private static String getLogGroupArn(CloudWatchLogsAsyncClient cloudWatchLogsAsyncClient, String logGroupName, String renderedRegion) throws InterruptedException, ExecutionException {
        return cloudWatchLogsAsyncClient.describeLogGroups(
                DescribeLogGroupsRequest.builder()
                    .logGroupNamePrefix(logGroupName)
                    .build()
            ).get().logGroups().stream()
            .filter(logGroup -> logGroup.arn().contains(renderedRegion))
            .findFirst()
            .map(LogGroup::arn)
            .map(arn -> arn.endsWith("*") ? arn.substring(0, arn.length() - 1) : arn)
            .orElse(null);
    }

    private TaskContainerProperties.Builder withResources(TaskContainerProperties.Builder builder, Integer memory, Float cpu) {
        return builder
            .resourceRequirements(
                ResourceRequirement.builder()
                    .type(ResourceType.MEMORY)
                    .value(memory.toString())
                    .build(),
                ResourceRequirement.builder()
                    .type(ResourceType.VCPU)
                    .value(cpu.toString())
                    .build()
            );
    }

    private void cleanupS3Resources(RunContext runContext, Path batchWorkingDirectory) throws IllegalVariableEvaluationException {
        String renderedBucket = runContext.render(bucket);
        try (S3AsyncClient s3AsyncClient = asyncClient(runContext)) {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(renderedBucket)
                .prefix(batchWorkingDirectory.toString())
                .build();

            ListObjectsV2Response listResponse = s3AsyncClient.listObjectsV2(listRequest).get();
            List<ObjectIdentifier> objectsIdentifiers = listResponse.contents().stream()
                .map(S3Object::key)
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .toList();
            StreamSupport.stream(Iterables.partition(
                    objectsIdentifiers,
                    1000
                ).spliterator(), false)
                .map(objects -> s3AsyncClient.deleteObjects(
                    DeleteObjectsRequest.builder()
                        .bucket(renderedBucket)
                        .delete(Delete.builder()
                            .objects(objects)
                            .build())
                        .build()
                )).forEach(throwConsumer(CompletableFuture::get));
        } catch (Exception e) {
            runContext.logger().warn("Error while cleaning up the S3 bucket: {}", e.toString());
        }
    }

    private S3TransferManager transferManager(RunContext runContext) throws IllegalVariableEvaluationException {
        return S3TransferManager.builder()
            .s3Client(this.asyncClient(runContext))
            .build();
    }

    private static StartLiveTailResponseHandler getStartLiveTailResponseStreamHandler(AbstractLogConsumer logConsumer) {
        return StartLiveTailResponseHandler.builder()
            .onError(throwable -> {
                CloudWatchLogsException e = (CloudWatchLogsException) throwable.getCause();
                logConsumer.accept(e.awsErrorDetails().errorMessage(), true);
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
                    } else if (event instanceof LiveTailSessionUpdate sessionUpdate) {
                        List<LiveTailSessionLogEvent> logEvents = sessionUpdate.sessionResults();
                        logEvents.stream()
                            .<String>mapMulti((e, consumer) -> e.message()
                                .replaceAll("(?!^)(::\\{)", System.lineSeparator() + "::{")
                                .replace("\r", System.lineSeparator())
                                .lines()
                                .forEach(consumer)
                            ).forEach(message -> {
                                if (message.startsWith("::{")) {
                                    logConsumer.accept(message, false);
                                } else {
                                    logConsumer.accept("[JOB LOG] " + message, false);
                                }
                            });
                    } else {
                        throw CloudWatchLogsException.builder().message("Unknown event type").build();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    logConsumer.accept(throwable.getMessage(), true);
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

    private void cleanupBatchResources(BatchClient client, String jobQueue, String jobDefArn) throws TimeoutException {
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

    private void safelyKillJob(final RunContext runContext, final Region region, final String jobId) {
        // Use a dedicated BatchClient, as the one used in the run method may be closed in the meantime.
        try (BatchClient client = newBatchClient(runContext, region)) {
            final DescribeJobsResponse response = client.describeJobs(
                DescribeJobsRequest.builder()
                    .jobs(jobId)
                    .build()
            );
            if (response.hasJobs()) {
                JobDetail jobDetail = response.jobs().get(0);
                if (!jobDetail.isTerminated() && !jobDetail.isCancelled()) {
                    try {
                        client.terminateJob(TerminateJobRequest
                            .builder()
                            .jobId(jobId)
                            .reason("Kestra task was killed.")
                            .build()
                        );
                        runContext.logger().debug("Job terminated: {}", jobId);
                    } catch (Exception e) {
                        runContext.logger().warn("Failed to cancel job: {}", jobId);
                    }
                }
            }
        } catch (IllegalVariableEvaluationException e) {
            throw new RuntimeException(e); // cannot happen here.
        }
    }

    @Getter
    @Builder
    public static class Resources {
        @NotNull
        private Resource request;
    }

    @Getter
    @Builder
    public static class Resource {
        @NotNull
        private String memory;
        @NotNull
        private String cpu;
    }
}

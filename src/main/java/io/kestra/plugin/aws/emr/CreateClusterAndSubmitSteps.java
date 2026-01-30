package io.kestra.plugin.aws.emr;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.plugin.aws.emr.models.StepConfig;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.emr.model.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create an EMR cluster and run steps",
    description = "Launches a new EMR cluster, optionally submits initial steps, and returns the jobFlowId. Defaults: releaseLabel emr-5.20.0, serviceRole EMR_DefaultRole, jobFlowRole EMR_EC2_DefaultRole, visibleToAllUsers true, keepJobFlowAliveWhenNoSteps false. Can wait until the cluster reaches WAITING or TERMINATED, polling every completionCheckInterval until waitUntilCompletion."
)
@Plugin(
    examples = {
        @Example(
            title = "Create an EMR Cluster, submit a Spark job, wait until the job is terminated.",
            full = true,
            code = """
                id: aws_emr_create_cluster
                namespace: company.team

                tasks:
                  - id: create_cluster
                    type: io.kestra.plugin.aws.emr.CreateClusterAndSubmitSteps
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: eu-west-3
                    clusterName: "Spark job cluster"
                    logUri: "s3://my-bucket/test-emr-logs"
                    keepJobFlowAliveWhenNoSteps: true
                    applications:
                        - Spark
                    masterInstanceType: m5.xlarge
                    slaveInstanceType: m5.xlarge
                    instanceCount: 3
                    ec2KeyName: my-ec2-ssh-key-pair-name
                    steps:
                        - name: Spark_job_test
                          jar: "command-runner.jar"
                          actionOnFailure: CONTINUE
                          commands:
                            - spark-submit s3://mybucket/health_violations.py --data_source s3://mybucket/food_establishment_data.csv --output_uri s3://mybucket/test-emr-output
                    wait: true
                """
        )
    }
)
public class CreateClusterAndSubmitSteps extends AbstractEmrTask implements RunnableTask<CreateClusterAndSubmitSteps.Output> {

    @Schema(
        title = "Cluster name",
        description = "Name assigned to the EMR cluster."
    )
    @NotNull
    private Property<String> clusterName;

    @Schema(
        title = "Release label",
        description = "EMR release version such as emr-6.15.0; default emr-5.20.0."
    )
    @NotNull
    @Builder.Default
    private Property<String> releaseLabel = Property.ofValue("emr-5.20.0");

    @Schema(
        title = "Steps to run",
        description = "Optional steps submitted with RunJobFlow; executed in order."
    )
    private List<StepConfig> steps;

    @Schema(
        title = "Applications",
        description = "EMR applications to install, e.g., Hive, Spark, Ganglia."
    )
    private Property<List<String>> applications;

    @Schema(
        title = "Log URI",
        description = "S3 URI for cluster logs; leave empty to disable logging."
    )
    private Property<String> logUri;

    @Schema(
        title = "Job flow role",
        description = "EC2 instance profile for the cluster nodes; default EMR_EC2_DefaultRole and must pre-exist."
    )
    @NotNull
    @Builder.Default
    private Property<String> jobFlowRole = Property.ofValue("EMR_EC2_DefaultRole");

    @Schema(
        title = "Visible to all users",
        description = "When true (default), any IAM principal in the account with permissions can manage the cluster."
    )
    @Builder.Default
    private Property<Boolean> visibleToAllUsers = Property.ofValue(true);

    @Schema(
        title = "Service role",
        description = "IAM role assumed by EMR to access AWS resources; default EMR_DefaultRole."
    )
    @NotNull
    @Builder.Default
    private Property<String> serviceRole = Property.ofValue("EMR_DefaultRole");

    @Schema(
        title = "Master instance type",
        description = "EC2 instance type for the primary node."
    )
    @NotNull
    private Property<String> masterInstanceType;

    @Schema(
        title = "Core/Task instance type",
        description = "EC2 instance type for core/task nodes."
    )
    @NotNull
    private Property<String> slaveInstanceType;

    @Schema(
        title = "Keep cluster alive",
        description = "If true, cluster stays in WAITING after steps; default false terminates when done."
    )
    @Builder.Default
    private Property<Boolean> keepJobFlowAliveWhenNoSteps = Property.ofValue(false);

    @Schema(
        title = "Instance count",
        description = "Total number of instances in the cluster."
    )
    @NotNull
    private Property<Integer> instanceCount;

    @Schema(
        title = "EC2 key pair",
        description = "Existing EC2 key pair for SSH access to the master as user hadoop."
    )
    private Property<String> ec2KeyName;

    @Schema(
        title = "EC2 subnet ID",
        description = """
        Applies to clusters that use the uniform instance group configuration.
        To launch the cluster in Amazon Virtual Private Cloud (Amazon VPC), set this parameter to the identifier of the Amazon VPC subnet where you want the cluster to launch.
        If you do not specify this value and your account supports EC2-Classic, the cluster launches in EC2-Classic.
        """
    )
    private Property<String> ec2SubnetId;

    @Schema(
        title = "Wait for completion",
        description = "When true, poll cluster status until TERMINATED, TERMINATED_WITH_ERRORS, or WAITING; default false."
    )
    @Builder.Default
    private Property<Boolean> wait = Property.ofValue(Boolean.FALSE);

    @Schema(
        title = "Check interval",
        description = "Polling frequency while waiting; default 10s."
    )
    @Builder.Default
    private Property<Duration> completionCheckInterval = Property.ofValue(Duration.ofSeconds(10));

    @Schema(
        title = "Completion timeout",
        description = "Maximum time to wait for WAITING or TERMINATED; default 1h."
    )
    @Builder.Default
    private Property<Duration> waitUntilCompletion = Property.ofValue(Duration.ofHours(1));

    @Override
    public Output run(RunContext runContext) throws IllegalVariableEvaluationException {
        try (var emrClient = this.emrClient(runContext)) {
            //Create Applications to be loaded
            List<Application> applicationsList = runContext.render(this.applications).asList(String.class)
                .stream()
                .map(name -> Application.builder().name(name).build()).toList();

            //Create instances configuration
            JobFlowInstancesConfig instancesConfig = JobFlowInstancesConfig.builder()
                .ec2SubnetId(runContext.render(this.ec2SubnetId).as(String.class).orElse(null))
                .ec2KeyName(runContext.render(this.ec2KeyName).as(String.class).orElse(null))
                .instanceCount(runContext.render(this.instanceCount).as(Integer.class).orElseThrow())
                .masterInstanceType(runContext.render(this.masterInstanceType).as(String.class).orElse(null))
                .slaveInstanceType(runContext.render(this.slaveInstanceType).as(String.class).orElse(null))
                .keepJobFlowAliveWhenNoSteps(runContext.render(this.keepJobFlowAliveWhenNoSteps).as(Boolean.class).orElseThrow())
                .build();

            RunJobFlowRequest jobFlowRequest = RunJobFlowRequest.builder()
                .name(runContext.render(this.clusterName).as(String.class).orElseThrow())
                .releaseLabel(runContext.render(this.releaseLabel).as(String.class).orElseThrow())
                .applications(applicationsList)
                .logUri(runContext.render(this.logUri).as(String.class).orElse(null))
                .serviceRole(runContext.render(this.serviceRole).as(String.class).orElse(null))
                .jobFlowRole(runContext.render(this.jobFlowRole).as(String.class).orElse(null))
                .instances(instancesConfig)
                .steps(steps == null ? null : steps.stream()
                    .map(throwFunction(step -> step.toStep(runContext)))
                    .toList()
                )
                .visibleToAllUsers(runContext.render(this.visibleToAllUsers).as(Boolean.class).orElseThrow())
                .build();

            RunJobFlowResponse response = emrClient.runJobFlow(jobFlowRequest);
            String jobFlowId = response.jobFlowId();

            runContext.logger().info("Cluster created with id : {}", jobFlowId);

            if (!Boolean.TRUE.equals(runContext.render(this.wait).as(Boolean.class).orElseThrow())) {
                return Output.builder()
                    .jobFlowId(jobFlowId)
                    .build();
            }

            final AtomicReference<DescribeClusterResponse> responseReference = new AtomicReference<>();
            try {
                Await.until(
                    () -> {
                        responseReference.set(emrClient.describeCluster(r -> r.clusterId(jobFlowId)));
                        ClusterState clusterState = responseReference.get().cluster().status().state();

                        return ClusterState.TERMINATED.equals(clusterState) ||
                            ClusterState.TERMINATED_WITH_ERRORS.equals(clusterState) ||
                            ClusterState.WAITING.equals(clusterState);
                    },
                    runContext.render(this.completionCheckInterval).as(Duration.class).orElseThrow(),
                    runContext.render(this.waitUntilCompletion).as(Duration.class).orElseThrow()
                );
            } catch (TimeoutException e) {
                throw new RuntimeException(responseReference.get().cluster().status().stateAsString());
            }

            runContext.logger().info("Cluster terminated with status : {}", responseReference.get().cluster().status().stateAsString());

            return Output.builder()
                .jobFlowId(jobFlowId)
                .build();
        }
    }

    @Builder
    @Getter
    static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Job flow ID",
            description = "Identifier of the created cluster."
        )
        private String jobFlowId;
    }
}

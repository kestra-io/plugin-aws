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
    title = "Create an EMR Cluster, submit steps to be processed, then get the cluster ID as an output."
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
                    type: io.kestra.plugin.aws.emr.CreateCluster
                    accessKeyId: <access-key>
                    secretKeyId: <secret-key>
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
public class CreateCluster extends AbstractEmrTask implements RunnableTask<CreateCluster.Output> {

    @Schema(title = "Cluster Name.")
    @NotNull
    private Property<String> clusterName;

    @Schema(title = "Release Label.", description = "It specifies the EMR release version label. Pattern is 'emr-x.x.x'.")
    @NotNull
    @Builder.Default
    private Property<String> releaseLabel = Property.of("emr-5.20.0");

    @Schema(
        title = "Steps",
        description = "List of steps to run."
    )
    private List<StepConfig> steps;

    @Schema(title = "Applications.", description = "List of applications name: Ex: \"Hive\", \"Spark\", \"Ganglia\"")
    private Property<List<String>> applications;

    @Schema(title = "Log URI.", description = "The location in Amazon S3 to write the log files of the job flow. If a value is not provided, logs are not created.")
    private Property<String> logUri;

    @Schema(title = "Job flow Role.", description = """
        Also called instance profile and Amazon EC2 role. An IAM role for an Amazon EMR cluster.
        The Amazon EC2 instances of the cluster assume this role. The default role is EMR_EC2_DefaultRole.
        In order to use the default role, you must have already created it using the CLI or console.
        """)
    @NotNull
    @Builder.Default
    private Property<String> jobFlowRole = Property.of("EMR_EC2_DefaultRole");

    @Schema(title = "Visible to all users.", description = "Set this value to true so that IAM principals in the Amazon Web Services account associated with the cluster can perform Amazon EMR actions on the cluster that their IAM policies allow.")
    @Builder.Default
    private Property<Boolean> visibleToAllUsers = Property.of(true);

    @Schema(title = "Service Role.", description = """
        The IAM role that Amazon EMR assumes in order to access Amazon Web Services resources on your behalf.
        If you've created a custom service role path, you must specify it for the service role when you launch your cluster.
        """)
    @NotNull
    @Builder.Default
    private Property<String> serviceRole = Property.of("EMR_DefaultRole");

    @NotNull
    @Schema(title = "Master Instance Type.", description = "EC2 instance type for master instances.")
    private Property<String> masterInstanceType;

    @NotNull
    @Schema(title = "Slave Instance Type.", description = "EC2 instance type for slave instances.")
    private Property<String> slaveInstanceType;

    @Schema(title = "Keep job flow alive.", description = "Specifies whether the cluster should remain available after completing all steps. Defaults to false.")
    @Builder.Default
    private Property<Boolean> keepJobFlowAliveWhenNoSteps = Property.of(false);

    @Schema(title = "Instance count.")
    @NotNull
    private Property<Integer> instanceCount;

    @Schema(title = "EC2 Key name.", description = "The name of the Amazon EC2 key pair that can be used to connect to the master node using SSH as the user called \"hadoop\".")
    private Property<String> ec2KeyName;

    @Schema(
        title = "EC2 Subnet ID.",
        description = """
        Applies to clusters that use the uniform instance group configuration.
        To launch the cluster in Amazon Virtual Private Cloud (Amazon VPC), set this parameter to the identifier of the Amazon VPC subnet where you want the cluster to launch.
        If you do not specify this value and your account supports EC2-Classic, the cluster launches in EC2-Classic.
        """
    )
    private Property<String> ec2SubnetId;

    @Schema(
        title = "Wait for the end of the run.",
        description = "If set to true it will wait until the cluster has status TERMINATED or WAITING."
    )
    @Builder.Default
    private Property<Boolean> wait = Property.of(Boolean.FALSE);

    @Schema(
        title = "Check interval duration.",
        description = "The frequency with which the task checks whether the job is completed."
    )
    @Builder.Default
    private Property<Duration> completionCheckInterval = Property.of(Duration.ofSeconds(10));

    @Schema(
        title = "Completion timeout."
    )
    @Builder.Default
    private Property<Duration> waitUntilCompletion = Property.of(Duration.ofHours(1));

    @Override
    public Output run(RunContext runContext) throws IllegalVariableEvaluationException {
        try(var emrClient = this.client(runContext)) {
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
        @Schema(title = "Job flow ID.")
        private String jobFlowId;
    }
}

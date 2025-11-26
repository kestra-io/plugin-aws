package io.kestra.plugin.aws.emr;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.emr.models.StepConfig;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
@Disabled("Provide credentials, a s3 bucket, and ec2 ssh key pair name to run the tests.")
class EmrIntegrationTest {
    @Inject
    private RunContextFactory runContextFactory;

    /*
      To retrieve tests data such as the csv and the python file, and set up the environment
      please go the AWS user guide https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html
     */

    private String accessKeyId = "";
    private String secretKeyId = "";
    private String sessionToken = "";
    private String region = "";
    private String bucketName = "";
    private String ec2KeyPairName = "";

    @Test
    void createCluster() throws Exception {
        CreateClusterAndSubmitSteps createCluster = CreateClusterAndSubmitSteps.builder()
            .accessKeyId(Property.ofValue(accessKeyId))
            .secretKeyId(Property.ofValue(secretKeyId))
            .sessionToken(Property.ofValue(sessionToken))
            .region(Property.ofValue(region))
            .clusterName(Property.ofValue("UNIT_TEST_CLUSTER"))
            .steps(List.of(createPythonSparkJob()))
            .logUri(Property.ofValue("s3://" + bucketName + "/test-emr-logs"))
            .keepJobFlowAliveWhenNoSteps(Property.ofValue(true))
            .applications(Property.ofValue(List.of("Spark")))
            .masterInstanceType(Property.ofValue("m5.xlarge"))
            .slaveInstanceType(Property.ofValue("m5.xlarge"))
            .instanceCount(Property.ofValue(3))
            .ec2KeyName(Property.ofValue(ec2KeyPairName))
            .wait(Property.ofValue(false))
            .keepJobFlowAliveWhenNoSteps(Property.ofValue(true))
            .build();

        CreateClusterAndSubmitSteps.Output output = createCluster.run(runContextFactory.of());
        assertNotNull(output.getJobFlowId());
    }

    @Test
    void deleteCluster() throws Exception {
        DeleteCluster deleteCluster = DeleteCluster.builder()
            .accessKeyId(Property.ofValue(accessKeyId))
            .secretKeyId(Property.ofValue(secretKeyId))
            .sessionToken(Property.ofValue(sessionToken))
            .region(Property.ofValue(region))
            .clusterIds(Property.ofValue(List.of("j-3B0V9K83SNI3M")))
            .build();

        deleteCluster.run(runContextFactory.of());
    }

    @Test
    void addStepsToCluster() throws Exception {
        SubmitSteps addJobFlowsSteps = SubmitSteps.builder()
            .accessKeyId(Property.ofValue(accessKeyId))
            .secretKeyId(Property.ofValue(secretKeyId))
            .sessionToken(Property.ofValue(sessionToken))
            .region(Property.ofValue(region))
            .clusterId(Property.ofValue("j-FOKPVZGD5FPI"))
            .steps(List.of(createPythonSparkJob()))
            .build();

        addJobFlowsSteps.run(runContextFactory.of());
    }

    private StepConfig createPythonSparkJob() {
        return StepConfig.builder()
            .jar(Property.ofValue("command-runner.jar"))
            .commands(Property.ofValue(
                List.of("spark-submit s3://" + bucketName + "/health_violations.py --data_source s3://"
                    + bucketName + "/food_establishment_data.csv --output_uri s3://" + bucketName + "/test-emr-output")
            ))
            .name(Property.ofValue("TEST SPARK JOB UNIT TEST"))
            .actionOnFailure(Property.ofValue(StepConfig.Action.CONTINUE))
            .build();
    }
}
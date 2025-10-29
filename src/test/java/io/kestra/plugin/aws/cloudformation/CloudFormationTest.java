package io.kestra.plugin.aws.cloudformation;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import java.util.UUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@Testcontainers 
class CloudFormationTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Container
    private static final LocalStackContainer localstack = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:2.2.0")
    ).withServices(LocalStackContainer.Service.CLOUDFORMATION); 

    private final String templateBody = """
        AWSTemplateFormatVersion: '2010-09-09'
        Description: A test S3 bucket.
        Resources:
          MyTestBucket:
            Type: 'AWS::S3::Bucket'
        """;

    @Test
    void createAndDeleteStackTest() throws Exception {
        RunContext runContext = runContextFactory.of();
        String stackName = "kestra-test-stack-" + UUID.randomUUID().toString().substring(0, 8);
       
        Create create = Create.builder()
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey())) 
            .secretKeyId(Property.of(localstack.getSecretKey())) 
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.CLOUDFORMATION).toString())) // IMPORTANT: Point to LocalStack
            .stackName(Property.of(stackName))
            .templateBody(Property.of(templateBody))
            .waitForCompletion(true)
            .build();

        Create.Output createOutput = create.run(runContext);
        
        assertThat(createOutput.getStackId(), is(notNullValue()));

        Delete delete = Delete.builder()
            .region(Property.of(localstack.getRegion()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.CLOUDFORMATION).toString())) // IMPORTANT: Point to LocalStack
            .stackName(Property.of(stackName))
            .waitForCompletion(true)
            .build();

        delete.run(runContext);
    }
}
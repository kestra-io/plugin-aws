package io.kestra.plugin.aws.cloudformation;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@Disabled("This test requires a real AWS account; provide credentials and region to run.")
class CloudFormationIntegrationTest {

    @Inject
    private RunContextFactory runContextFactory;

    // --- Configuration: Fill these in before running ---
    private String accessKeyId = "YOUR_AWS_ACCESS_KEY_ID";
    private String secretKeyId = "YOUR_AWS_SECRET_KEY_ID";
    private String region = "us-east-1";
    
    private String templateBody = """
        AWSTemplateFormatVersion: '2010-09-09'
        Resources:
          MyTestBucket:
            Type: 'AWS::S3::Bucket'
        """;

    @Test
    void createStackTest() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String stackName = "kestra-test-stack-" + UUID.randomUUID().toString().substring(0, 8);

        CreateStack task = CreateStack.builder()
            .accessKeyId(Property.of(accessKeyId))
            .secretKeyId(Property.of(secretKeyId))
            .region(Property.of(region))
            .stackName(Property.of(stackName))
            .templateBody(Property.of(templateBody))
            .waitForCompletion(true)
            .build();

        CreateStack.Output output = task.run(runContext);

        assertThat(output.getStackId(), is(notNullValue()));
    }
    
    @Test
    void deleteStackTest() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        // IMPORTANT: Use a stack name that you know exists from a previous test run
        String stackNameToDelete = "kestra-test-stack-......"; 

        DeleteStack task = DeleteStack.builder()
            .accessKeyId(Property.of(accessKeyId))
            .secretKeyId(Property.of(secretKeyId))
            .region(Property.of(region))
            .stackName(Property.of(stackNameToDelete))
            .waitForCompletion(true)
            .build();

        DeleteStack.Output output = task.run(runContext);

        assertThat(output.getStackName(), is(stackNameToDelete));
    }
}
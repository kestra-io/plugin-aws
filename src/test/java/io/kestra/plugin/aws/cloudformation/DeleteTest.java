package io.kestra.plugin.aws.cloudformation;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractLocalStackTest;

import jakarta.inject.Inject;

class DeleteTest extends AbstractLocalStackTest {

    @Inject
    private RunContextFactory runContextFactory;

    private final String templateBody = """
        AWSTemplateFormatVersion: '2010-09-09'
        Description: A test S3 bucket to be deleted.
        Resources:
          MyTestBucket:
            Type: 'AWS::S3::Bucket'
        """;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();
        String stackName = "kestra-delete-test-stack-" + UUID.randomUUID().toString().substring(0, 8);

        Create create = Create.builder()
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .stackName(Property.ofValue(stackName))
            .templateBody(Property.ofValue(templateBody))
            .waitForCompletion(Property.ofValue(true))
            .build();
        create.run(runContext);

        Delete delete = Delete.builder()
            .region(Property.ofValue(REGION))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .stackName(Property.ofValue(stackName))
            .waitForCompletion(Property.ofValue(true))
            .build();
        delete.run(runContext);
    }
}

package io.kestra.plugin.aws.cloudformation;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.*;
import software.amazon.awssdk.services.cloudformation.waiters.CloudFormationWaiter;

import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create or update a CloudFormation stack."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a simple S3 bucket with CloudFormation and wait for completion.",
            full = true,
            code = """
                id: aws_cfn_create_stack
                namespace: dev
                tasks:
                  - id: create_s3_bucket
                    type: io.kestra.plugin.aws.cloudformation.CreateStack
                    accessKeyId: "{{ secrets.AWS_ACCESS_KEY_ID }}"
                    secretKeyId: "{{ secrets.AWS_SECRET_ACCESS_KEY }}"
                    region: "us-east-1"
                    stackName: "my-s3-bucket-stack"
                    waitForCompletion: true
                    templateBody: |
                      AWSTemplateFormatVersion: '2010-09-09'
                      Description: A simple S3 bucket
                      Resources:
                        MyS3Bucket:
                          Type: 'AWS::S3::Bucket'
                          Properties:
                            BucketName: 'my-unique-kestra-bucket-1'
                      Outputs:
                        BucketName:
                          Value: !Ref MyS3Bucket
                          Description: Name of the S3 bucket created
                """
        )
    }
)
public class CreateStack extends AbstractCloudFormation implements RunnableTask<CreateStack.Output> {

    @PluginProperty(dynamic = true)
    @NotNull
    @Schema(title = "The name of the stack.")
    private Property<String> stackName;

    @PluginProperty(dynamic = true)
    @Schema(title = "The structure that contains the stack template.")
    private Property<String> templateBody;

    @PluginProperty
    @Schema(title = "A list of Parameter structures for the stack.")
    private Property<Map<String, String>> parameters;

    @PluginProperty(dynamic = false)
    @Builder.Default
    @Schema(title = "Whether to wait for the stack operation to complete.")
    private Boolean waitForCompletion = true;

    @Override
    public Output run(RunContext runContext) throws Exception {
        CloudFormationClient cfClient = this.cfClient(runContext);
        String renderedStackName = runContext.render(this.stackName).as(String.class).orElseThrow();

        boolean stackExists = cfClient.listStacks(ListStacksRequest.builder()
                .stackStatusFilters(StackStatus.CREATE_COMPLETE, StackStatus.UPDATE_COMPLETE, StackStatus.UPDATE_ROLLBACK_COMPLETE)
                .build())
            .stackSummaries().stream().anyMatch(s -> s.stackName().equals(renderedStackName));

        if (stackExists) {
            runContext.logger().info("Stack '{}' already exists. Attempting to update.", renderedStackName);
            UpdateStackRequest.Builder updateRequestBuilder = UpdateStackRequest.builder().stackName(renderedStackName);
            
            if (this.templateBody != null) {
                 updateRequestBuilder.templateBody(runContext.render(this.templateBody).as(String.class).orElse(null));
            }
            if (this.parameters != null) {
                updateRequestBuilder.parameters(mapToParameters(runContext, runContext.render(this.parameters).asMap(String.class, String.class)));
            }
            
            cfClient.updateStack(updateRequestBuilder.build());

            if (this.waitForCompletion) {
                try (CloudFormationWaiter waiter = cfClient.waiter()) {
                    waiter.waitUntilStackUpdateComplete(s -> s.stackName(renderedStackName));
                }
            }

        } else {
            runContext.logger().info("Stack '{}' does not exist. Creating new stack.", renderedStackName);
            CreateStackRequest.Builder createRequestBuilder = CreateStackRequest.builder().stackName(renderedStackName);

            if (this.templateBody != null) {
                 createRequestBuilder.templateBody(runContext.render(this.templateBody).as(String.class).orElse(null));
            }
            if (this.parameters != null) {
                createRequestBuilder.parameters(mapToParameters(runContext, runContext.render(this.parameters).asMap(String.class, String.class)));
            }

            cfClient.createStack(createRequestBuilder.build());

            if (this.waitForCompletion) {
                try (CloudFormationWaiter waiter = cfClient.waiter()) {
                    waiter.waitUntilStackCreateComplete(s -> s.stackName(renderedStackName));
                }
            }
        }

        DescribeStacksResponse describeStacksResponse = cfClient.describeStacks(DescribeStacksRequest.builder().stackName(renderedStackName).build());
        Stack finalStack = describeStacksResponse.stacks().get(0);
        
        return Output.builder()
            .stackId(finalStack.stackId())
            .stackOutputs(finalStack.outputs().stream().collect(Collectors.toMap(
                software.amazon.awssdk.services.cloudformation.model.Output::outputKey,
                software.amazon.awssdk.services.cloudformation.model.Output::outputValue
            )))
            .build();
    }

    private List<Parameter> mapToParameters(RunContext runContext, Map<String, String> params) throws IllegalVariableEvaluationException {
        if (params == null) {
            return null;
        }
        return params.entrySet().stream()
            .map(throwFunction(entry -> Parameter.builder()
                .parameterKey(entry.getKey())
                .parameterValue(runContext.render(entry.getValue()))
                .build()))
            .collect(Collectors.toList());
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The unique stack ID.")
        private final String stackId;
        @Schema(title = "A map of the stack's outputs.")
        private final Map<String, String> stackOutputs;
    }
}
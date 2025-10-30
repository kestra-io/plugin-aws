package io.kestra.plugin.aws.cloudformation;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.*;
import software.amazon.awssdk.services.cloudformation.waiters.CloudFormationWaiter;

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
            full = true,
            title = "Create a simple S3 bucket with CloudFormation and wait for completion.",
            code = """
                id: aws_cfn_create_stack
                namespace: dev

                tasks:
                  - id: create_s3_bucket
                    type: io.kestra.plugin.aws.cloudformation.Create
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
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
                            BucketName: "kestra-cfn-test-1"
                      Outputs:
                        BucketName:
                          Value: !Ref MyS3Bucket
                          Description: Name of the S3 bucket created
                """
        )
    }
)
public class Create extends AbstractCloudFormation implements RunnableTask<Create.Output> {

    @Schema(title = "The structure that contains the stack template.")
    private Property<String> templateBody;

    @Schema(title = "A list of Parameter structures for the stack.")
    private Property<Map<String, String>> parameters;

    @Override
    public Output run(RunContext runContext) throws Exception {
        CloudFormationClient cfClient = this.cfClient(runContext);
        String rStackName = runContext.render(this.stackName).as(String.class).orElseThrow();
        Boolean rWaitForCompletion = runContext.render(this.waitForCompletion).as(Boolean.class).orElse(true);

        boolean stackExists = cfClient.listStacks(ListStacksRequest.builder()
                .stackStatusFilters(StackStatus.CREATE_COMPLETE, StackStatus.UPDATE_COMPLETE, StackStatus.UPDATE_ROLLBACK_COMPLETE)
                .build())
            .stackSummaries().stream().anyMatch(s -> s.stackName().equals(rStackName));

        if (stackExists) {
            runContext.logger().info("Stack '{}' already exists. Attempting to update.", rStackName);
            UpdateStackRequest.Builder updateRequestBuilder = UpdateStackRequest.builder().stackName(rStackName);

            if (this.templateBody != null) {
                updateRequestBuilder.templateBody(runContext.render(this.templateBody).as(String.class).orElse(null));
            }
            if (this.parameters != null) {
                updateRequestBuilder.parameters(mapToParameters(runContext, runContext.render(this.parameters).asMap(String.class, String.class)));
            }

            cfClient.updateStack(updateRequestBuilder.build());

          
        } else {
            runContext.logger().info("Stack '{}' does not exist. Creating new stack.", rStackName);
            CreateStackRequest.Builder createRequestBuilder = CreateStackRequest.builder().stackName(rStackName);

            if (this.templateBody != null) {
                createRequestBuilder.templateBody(runContext.render(this.templateBody).as(String.class).orElse(null));
            }
            if (this.parameters != null) {
                createRequestBuilder.parameters(mapToParameters(runContext, runContext.render(this.parameters).asMap(String.class, String.class)));
            }

            cfClient.createStack(createRequestBuilder.build());
        }

        if (rWaitForCompletion) {
            runContext.logger().info("Waiting for stack '{}' operation to complete.", rStackName);
            try (CloudFormationWaiter waiter = cfClient.waiter()) {
                if (stackExists) {
                    waiter.waitUntilStackUpdateComplete(r -> r.stackName(rStackName));
                } else {
                    waiter.waitUntilStackCreateComplete(r -> r.stackName(rStackName));
                }
            }
            catch (Exception e) {
                runContext.logger().error("Error while waiting for stack '{}' operation to complete: {}", rStackName, e.getMessage());
                throw e;
            }
        }

        runContext.logger().info("Stack '{}' operation completed.", rStackName);

        DescribeStacksResponse describeStacksResponse = cfClient.describeStacks(DescribeStacksRequest.builder().stackName(rStackName).build());
        Stack finalStack = describeStacksResponse.stacks().getFirst();

        return Output.builder()
            .stackId(finalStack.stackId())
            .stackName(finalStack.stackName())
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

        @Schema(title = "The name of the stack.") 
        private final String stackName;

        @Schema(title = "A map of the stack's outputs.")
        private final Map<String, String> stackOutputs;
    }
}
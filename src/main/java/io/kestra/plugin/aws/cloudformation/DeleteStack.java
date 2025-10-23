package io.kestra.plugin.aws.cloudformation;

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
import software.amazon.awssdk.services.cloudformation.model.DeleteStackRequest;
import software.amazon.awssdk.services.cloudformation.waiters.CloudFormationWaiter;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Delete a CloudFormation stack.")
@Plugin(
    examples = {
        @Example(
            title = "Delete a CloudFormation stack and wait for it to be fully removed.",
            full = true,
            code = """
                id: aws_cfn_delete_stack
                namespace: dev
                tasks:
                  - id: delete_my_stack
                    type: io.kestra.plugin.aws.cloudformation.DeleteStack
                    
                    # --- Authentication ---
                    # It's best practice to store credentials as Kestra secrets
                    accessKeyId: "{{ secrets.AWS_ACCESS_KEY_ID }}"
                    secretKeyId: "{{ secrets.AWS_SECRET_ACCESS_KEY }}"
                    region: "us-east-1"
                    
                    # --- Task Configuration ---
                    # The name of the stack you want to delete
                    stackName: "my-stack-to-delete"
                    
                    # This will make the task wait until the stack is fully deleted in AWS (optional, defaults to true)
                    waitForCompletion: true
                """
        )
    }
)
public class DeleteStack extends AbstractCloudFormation implements RunnableTask<DeleteStack.Output> {

    @PluginProperty(dynamic = true)
    @NotNull
    @Schema(title = "The name of the stack to delete.")
    private Property<String> stackName;

    @PluginProperty(dynamic = false)
    @Builder.Default
    @Schema(title = "Whether to wait for the stack deletion to complete.")
    private Boolean waitForCompletion = true;

    @Override
    public Output run(RunContext runContext) throws Exception {
        CloudFormationClient cfClient = this.cfClient(runContext);
        String renderedStackName = runContext.render(this.stackName).as(String.class).orElseThrow();

        DeleteStackRequest deleteRequest = DeleteStackRequest.builder()
            .stackName(renderedStackName)
            .build();

        cfClient.deleteStack(deleteRequest);

        if (this.waitForCompletion) {
            try (CloudFormationWaiter waiter = cfClient.waiter()) {
                waiter.waitUntilStackDeleteComplete(r -> r.stackName(renderedStackName));
            }
        }
        
        runContext.logger().info("Stack '{}' deletion process initiated.", renderedStackName);

        return Output.builder()
            .stackName(renderedStackName)
            .build();
    }
    
    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The name of the stack that was deleted."
        )
        private final String stackName;
    }
}
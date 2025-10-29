package io.kestra.plugin.aws.cloudformation;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.DeleteStackRequest;
import software.amazon.awssdk.services.cloudformation.waiters.CloudFormationWaiter;

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
                    type: io.kestra.plugin.aws.cloudformation.Delete
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    region: "us-east-1"
                    stackName: "my-stack-to-delete"
                    waitForCompletion: true
                """
        )
    }
)
public class Delete extends AbstractCloudFormation implements RunnableTask<VoidOutput> {


    @Builder.Default
    @Schema(title = "Whether to wait for the stack deletion to complete.")
    private Property<Boolean> waitForCompletion = Property.of(true);

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        CloudFormationClient cfClient = this.cfClient(runContext);
        String rStackName = runContext.render(this.stackName).as(String.class).orElseThrow();
        Boolean rWaitForCompletion = runContext.render(this.waitForCompletion).as(Boolean.class).orElse(true);
        
        runContext.logger().info("Attempting to delete CloudFormation stack '{}'", rStackName);

        DeleteStackRequest deleteRequest = DeleteStackRequest.builder()
            .stackName(rStackName)
            .build();

        cfClient.deleteStack(deleteRequest);

        if (rWaitForCompletion) {
            runContext.logger().info("Waiting for stack '{}' deletion to complete.", rStackName);
            try (CloudFormationWaiter waiter = cfClient.waiter()) {
                waiter.waitUntilStackDeleteComplete(r -> r.stackName(rStackName));
            }
            catch (Exception e) {
                runContext.logger().error("Error while waiting for stack '{}' deletion to complete: {}", rStackName, e.getMessage());
                throw e;
            }
        }

        runContext.logger().info("Successfully initiated deletion for stack '{}'", rStackName);

        return null;
    }
}
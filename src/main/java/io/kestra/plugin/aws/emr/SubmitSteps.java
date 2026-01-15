package io.kestra.plugin.aws.emr;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.emr.models.StepConfig;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Add steps to an existing AWS EMR cluster."
)
@Plugin(
    examples = {
        @Example(
            title = "Add a job step to an existing AWS EMR cluster",
            full = true,
            code = """
                id: aws_emr_add_emr_job_steps
                namespace: company.team

                tasks:
                  - id: add_steps_emr
                    type: io.kestra.plugin.aws.emr.SubmitSteps
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-west-3"
                    clusterId: j-XXXXXXXXXXXX
                    steps:
                        - name: Spark_job_test
                          jar: "command-runner.jar"
                          actionOnFailure: CONTINUE
                          commands:
                            - spark-submit s3://mybucket/health_violations.py --data_source s3://mybucket/food_establishment_data.csv --output_uri s3://mybucket/test-emr-output
                """
        )
    }
)
public class SubmitSteps extends AbstractEmrTask implements RunnableTask<VoidOutput> {
    @Schema(title = "Cluster ID.")
    @NotNull
    private Property<String> clusterId;

    @Schema(
        title = "Steps",
        description = "List of steps to add to the existing cluster."
    )
    @NotNull
    private List<StepConfig> steps;

    @Schema(
        title = "Execution role ARN.",
        description = """
        The Amazon Resource Name (ARN) of the runtime role for a step on the cluster. The runtime role can be a cross-account IAM role.
        The runtime role ARN is a combination of account ID, role name, and role type using the following format: arn:partition:service:region:account:resource.
        """
    )
    private Property<String> executionRoleArn;

    @Override
    public VoidOutput run(RunContext runContext) throws IllegalVariableEvaluationException {
        try(var emrClient = this.emrClient(runContext)) {
            List<software.amazon.awssdk.services.emr.model.StepConfig> jobSteps = steps.stream()
                .map(throwFunction(stepConfig -> stepConfig.toStep(runContext)))
                .toList();

            emrClient.addJobFlowSteps(throwConsumer(request -> request
                .steps(jobSteps)
                .jobFlowId(runContext.render(this.clusterId).as(String.class).orElseThrow())
                .executionRoleArn(runContext.render(this.executionRoleArn).as(String.class).orElse(null))
            ));
            return null;
        }
    }
}

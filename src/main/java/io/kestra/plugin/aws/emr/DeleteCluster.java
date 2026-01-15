package io.kestra.plugin.aws.emr;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.emr.EmrClient;

import java.util.List;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Terminate one or multiple AWS EMR cluster."
)
@Plugin(
    examples = {
        @Example(
            title = "Shutdown a couple of EMR cluster providing their IDs.",
            full = true,
            code = """
                id: aws_emr_delete_cluster
                namespace: company.team

                tasks:
                  - id: delete_cluster
                    type: io.kestra.plugin.aws.emr.DeleteCluster
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    clusterIds:
                        - j-XYXYXYXYXYXY
                        - J-XZXZXZXZXZXZ
                """
        )
    }
)
public class DeleteCluster extends AbstractEmrTask implements RunnableTask<VoidOutput> {
    @Schema(title = "Cluster IDs.", description = "List of cluster IDs to be terminated.")
    @NotNull
    private Property<List<String>> clusterIds;

    @Override
    public VoidOutput run(RunContext runContext) throws IllegalVariableEvaluationException {
        try (EmrClient emrClient = this.emrClient(runContext)) {
            emrClient.terminateJobFlows(throwConsumer(request -> request.jobFlowIds(runContext.render(this.clusterIds).asList(String.class))));
            return null;
        }
    }
}

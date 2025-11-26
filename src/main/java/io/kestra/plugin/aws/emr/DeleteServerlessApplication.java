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
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import java.util.List;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete one or multiple AWS EMR Serverless applications."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a couple of EMR Serverless applications providing their IDs.",
            full = true,
            code = """
                id: aws_emrserverless_delete_app
                namespace: company.team

                tasks:
                  - id: delete_app
                    type: io.kestra.plugin.aws.emr.DeleteServerlessApplication
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    applicationIds:
                        - 00f123abc456xyz
                        - 11g789def012uvw
                """
        )
    }
)
public class DeleteServerlessApplication extends AbstractEmrServerlessTask implements RunnableTask<VoidOutput> {

    @Schema(
        title = "Application IDs.",
        description = "List of EMR Serverless application IDs to delete."
    )
    @NotNull
    private Property<List<String>> applicationIds;

    @Override
    public VoidOutput run(RunContext runContext) throws IllegalVariableEvaluationException {
        try (EmrServerlessClient client = this.client(runContext)) {
            List<String> rApplicationIds = runContext.render(this.applicationIds).asList(String.class);
            rApplicationIds.forEach(throwConsumer(appId ->
                client.deleteApplication(r -> r.applicationId(appId))
            ));
            runContext.logger().info("Deleted {} EMR Serverless applications: {}", rApplicationIds.size(), rApplicationIds);
            return null;
        }
    }
}

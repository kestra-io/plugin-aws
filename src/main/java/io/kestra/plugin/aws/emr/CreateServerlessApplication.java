package io.kestra.plugin.aws.emr;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationResponse;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Create an EMR Serverless application.")
@Plugin(
    examples = {
        @Example(
            title = "Create an EMR Serverless application",
            full = true,
            code = """
                id: create_emr_serverless_app
                namespace: company.team

                tasks:
                  - id: create_app
                    type: io.kestra.plugin.aws.emr.CreateServerlessApplication
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    releaseLabel: "emr-7.0.0"
                    type: "SPARK"
                """
        )
    }
)
public class CreateServerlessApplication extends AbstractEmrServerlessTask implements RunnableTask<CreateServerlessApplication.Output> {

    @Schema(title = "The EMR release label, e.g. emr-7.0.0")
    @NotNull
    private Property<String> releaseLabel;

    @Schema(title = "Application type.", description = "SPARK or HIVE")
    @NotNull
    private Property<String> applicationType;

    @Override
    public Output run(RunContext runContext) throws IllegalVariableEvaluationException {
        try (EmrServerlessClient client = this.client(runContext)) {
            String release = runContext.render(this.releaseLabel).as(String.class).orElseThrow();
            String typeVal = runContext.render(this.applicationType).as(String.class).orElseThrow();

            CreateApplicationRequest request = CreateApplicationRequest.builder()
                .releaseLabel(release)
                .type(typeVal)
                .build();

            CreateApplicationResponse response = client.createApplication(request);

            runContext.logger().info("Created EMR Serverless Application: {}", response.applicationId());

            return Output.builder()
                .applicationId(response.applicationId())
                .build();

        } catch (Exception e) {
            throw new RuntimeException("Failed to create EMR Serverless application", e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Application ID.")
        private final String applicationId;
    }
}

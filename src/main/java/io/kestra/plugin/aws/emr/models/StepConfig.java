package io.kestra.plugin.aws.emr.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@Getter
@Builder
@EqualsAndHashCode
@Jacksonized
public class StepConfig {
    @Schema(title = "JAR path.", description = "A path to a JAR file run during the step.")
    @NotNull
    private Property<String> jar;

    @Schema(title = "Main class.", description = "The name of the main class in the specified Java file. If not specified, the JAR file should specify a Main-Class in its manifest file.")
    private Property<String> mainClass;

    @Schema(title = "Arguments." , description = "A list of command line arguments passed to the JAR file's main function when executed.")
    private Property<List<String>> arguments;

    @Schema(title = "Step configuration name.", description = "Ex: \"Run Spark job\"")
    @NotNull
    private Property<String> name;

    @Schema(title = "Action on failure.", description = "Possible values : TERMINATE_CLUSTER, CANCEL_AND_WAIT, CONTINUE, TERMINATE_JOB_FLOW.")
    @NotNull
    private Property<Action> actionOnFailure;

    public software.amazon.awssdk.services.emr.model.StepConfig toStep(RunContext runContext) throws IllegalVariableEvaluationException {
        return software.amazon.awssdk.services.emr.model.StepConfig.builder()
            .name(runContext.render(this.name).as(String.class).orElseThrow())
            .actionOnFailure(runContext.render(this.actionOnFailure).as(Action.class).orElseThrow().name())
            .hadoopJarStep(throwConsumer(hadoopJarStepBuilder ->
                hadoopJarStepBuilder.jar(runContext.render(this.jar).as(String.class).orElseThrow())
                    .mainClass(runContext.render(this.mainClass).as(String.class).orElse(null))
                    .args(runContext.render(this.arguments).asList(String.class))
                    .build()))
            .build();
    }

    public enum Action {
        TERMINATE_CLUSTER,
        CANCEL_AND_WAIT,
        CONTINUE,
        TERMINATE_JOB_FLOW
    }
}

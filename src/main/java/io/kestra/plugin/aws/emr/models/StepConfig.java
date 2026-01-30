package io.kestra.plugin.aws.emr.models;

import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@Getter
@Builder
@EqualsAndHashCode
@Jacksonized
public class StepConfig {
    @Schema(
        title = "JAR path",
        description = "JAR executed for the step, e.g., command-runner.jar."
    )
    @NotNull
    private Property<String> jar;

    @Schema(
        title = "Main class",
        description = "Entry class name; omit if the JAR manifest defines Main-Class."
    )
    private Property<String> mainClass;

    @Schema(
        title = "Arguments",
        description = "List of arguments; each string is split on spaces before being passed to the step."
    )
    private Property<List<String>> commands;

    @Schema(
        title = "Step name",
        description = "Label for the step, e.g., Run Spark job."
    )
    @NotNull
    private Property<String> name;

    @Schema(
        title = "Action on failure",
        description = "Behavior when the step fails: TERMINATE_CLUSTER, CANCEL_AND_WAIT, CONTINUE, or TERMINATE_JOB_FLOW."
    )
    @NotNull
    private Property<Action> actionOnFailure;

    public software.amazon.awssdk.services.emr.model.StepConfig toStep(RunContext runContext) throws IllegalVariableEvaluationException {
        return software.amazon.awssdk.services.emr.model.StepConfig.builder()
            .name(runContext.render(this.name).as(String.class).orElseThrow())
            .actionOnFailure(runContext.render(this.actionOnFailure).as(Action.class).orElseThrow().name())
            .hadoopJarStep(throwConsumer(hadoopJarStepBuilder ->
                hadoopJarStepBuilder.jar(runContext.render(this.jar).as(String.class).orElseThrow())
                    .mainClass(runContext.render(this.mainClass).as(String.class).orElse(null))
                    .args(commandToAwsArguments(runContext.render(this.commands).asList(String.class)))
                    .build()))
            .build();
    }

    @VisibleForTesting
    static List<String> commandToAwsArguments(List<String> commands) {
        return commands.isEmpty() ? null : commands.stream()
            .map(command -> Arrays.stream(command.split(" ")).toList())
            .reduce(new ArrayList<>(), (acc, command) -> {
                acc.addAll(command);
                return acc;
            });
    }

    public enum Action {
        TERMINATE_CLUSTER,
        CANCEL_AND_WAIT,
        CONTINUE,
        TERMINATE_JOB_FLOW
    }
}

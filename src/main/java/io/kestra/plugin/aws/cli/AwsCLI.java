package io.kestra.plugin.aws.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.NamespaceFiles;
import io.kestra.core.models.tasks.NamespaceFilesInterface;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.services.ScriptService;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Execute aws commands."
)
@Plugin(
        examples = {
                @Example(
                        title = "Create a simple S3 bucket",
                        code = {
                                "accessKeyId: \"<access-key>\"",
                                "secretKeyId: \"<secret-key>\"",
                                "region: \"eu-central-1\"",
                                "commands:",
                                "  - aws s3 mb s3://test-bucket"
                        }
                ),
                @Example(
                        title = "List all S3 buckets as the task's output",
                        code = {
                                "accessKeyId: \"<access-key>\"",
                                "secretKeyId: \"<secret-key>\"",
                                "region: \"eu-central-1\"",
                                "commands:",
                                "  - aws s3api list-buckets | tr -d ' \\n' | xargs -0 -I {} echo '::{\"outputs\":{}}::'"
                        }
                )
        }
)
public class AwsCLI extends AbstractConnection implements RunnableTask<ScriptOutput>, NamespaceFilesInterface {
    private static final String DEFAULT_IMAGE = "amazon/aws-cli";

    @Schema(
        title = "The commands to run"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    protected List<String> commands;

    @Schema(
        title = "Additional environment variables for the current process."
    )
    @PluginProperty(
            additionalProperties = String.class,
            dynamic = true
    )
    protected Map<String, String> env;

    @Schema(
        title = "Docker options when for the `DOCKER` runner",
        defaultValue = "{image=" + DEFAULT_IMAGE + ", pullPolicy=ALWAYS}"
    )
    @PluginProperty
    @Builder.Default
    protected DockerOptions docker = DockerOptions.builder().build();

    @Schema(
        title = "Wanted output format for AWS commands (can be override with --format parameter)"
    )
    @PluginProperty
    @Builder.Default
    protected OutputFormat outputFormat = OutputFormat.JSON;

    private NamespaceFiles namespaceFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commands = new CommandsWrapper(runContext)
                .withWarningOnStdErr(true)
                .withRunnerType(RunnerType.DOCKER)
                .withDockerOptions(injectDefaults(getDocker()))
                .withCommands(
                        ScriptService.scriptCommands(
                                List.of("/bin/sh", "-c"),
                                null,
                                this.commands)
                );

        commands = commands.withEnv(this.getEnv(runContext))
            .withNamespaceFiles(namespaceFiles);

        return commands.run();
    }

    private DockerOptions injectDefaults(DockerOptions original) {
        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }
        if (original.getEntryPoint() == null || original.getEntryPoint().isEmpty()) {
            builder.entryPoint(List.of(""));
        }

        return builder.build();
    }

    private Map<String, String> getEnv(RunContext runContext) throws IllegalVariableEvaluationException {
        Map<String, String> envs = new HashMap<>();
        if (this.accessKeyId != null) {
            envs.put("AWS_ACCESS_KEY_ID", runContext.render(this.accessKeyId));
        }
        if (this.secretKeyId != null) {
            envs.put("AWS_SECRET_ACCESS_KEY", runContext.render(this.secretKeyId));
        }
        if (this.region != null) {
            envs.put("AWS_DEFAULT_REGION", runContext.render(this.region));
        }
        if (this.sessionToken != null) {
            envs.put("AWS_SESSION_TOKEN", runContext.render(this.sessionToken));
        }
        if (this.endpointOverride != null) {
            envs.put("AWS_ENDPOINT_URL", runContext.render(this.endpointOverride));
        }

        envs.put("AWS_DEFAULT_OUTPUT", this.outputFormat.toString());

        if (this.env != null) {
            envs.putAll(this.env);
        }

        return envs;
    }

    @Introspected
    public enum OutputFormat {
        JSON,
        TEXT,
        TABLE,
        YAML;


        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }
}

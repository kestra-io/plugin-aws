package io.kestra.plugin.aws.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Automate AWS services with the AWS CLI."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a simple AWS CLI command and capture the output.",
            full = true,
            code = """
                id: aws_cli
                namespace: company.team
                tasks:
                  - id: cli
                    type: io.kestra.plugin.aws.cli.AwsCLI
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    region: "us-east-1"
                    commands:
                      - aws sts get-caller-identity | tr -d ' \n' | xargs -0 -I {} echo '::{"outputs":{}}::'"""
        ),
        @Example(
            title = "Create a simple S3 bucket.",
            full = true,
            code = """
                id: aws_cli
                namespace: company.team

                tasks:
                  - id: cli
                    type: io.kestra.plugin.aws.cli.AwsCLI
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    commands:
                      - aws s3 mb s3://test-bucket
                """
        ),
        @Example(
            title = "List all S3 buckets as the task's output.",
            full = true,
            code = """
                id: aws_cli
                namespace: company.team

                tasks:
                  - id: cli
                    type: io.kestra.plugin.aws.cli.AwsCLI
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    commands:
                      - aws s3api list-buckets | tr -d ' \n' | xargs -0 -I {} echo '::{"outputs":{}}::'
                """
        )
    }
)
public class AwsCLI extends AbstractConnection implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {
    private static final String DEFAULT_IMAGE = "amazon/aws-cli";

    @Schema(
        title = "The AWS commands to run."
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
        title = "Deprecated, use 'taskRunner' instead"
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    private TaskRunner taskRunner = Docker.instance();

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String containerImage = DEFAULT_IMAGE;

    @Schema(
        title = "Expected output format for AWS commands (can be overridden with --format parameter)."
    )
    @PluginProperty
    @Builder.Default
    protected OutputFormat outputFormat = OutputFormat.JSON;

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    private List<String> outputFiles;

    private CredentialSource stsCredentialSource;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        List<String> allCommands = new ArrayList<>(this.commands);

        // hack for missing env vars supports: https://github.com/aws/aws-cli/issues/5639
        if (this.stsRoleArn != null) {
            allCommands.add("aws configure set role_arn " + runContext.render(this.stsRoleArn));
        }

        if (this.stsRoleSessionName != null) {
            allCommands.add("aws configure set role_session_name " + runContext.render(this.stsRoleSessionName));
        }

        if (this.stsRoleExternalId != null) {
            allCommands.add("aws configure set external_id " + runContext.render(this.stsRoleExternalId));
        }

        if (this.stsRoleSessionDuration != null) {
            allCommands.add("aws configure set duration_seconds " + stsRoleSessionDuration.getSeconds());
        }

        if (this.stsCredentialSource != null) {
            allCommands.add("aws configure set credential_source " + this.stsCredentialSource.value);
        }

        CommandsWrapper commands = new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withDockerOptions(injectDefaults(getDocker()))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withCommands(
                ScriptService.scriptCommands(
                    List.of("/bin/sh", "-c"),
                    null,
                    allCommands)
            )
            .withEnv(this.getEnv(runContext))
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(outputFiles);

        return commands.run();
    }

    private DockerOptions injectDefaults(DockerOptions original) {
        if (original == null) {
            return null;
        }

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
            envs.put("AWS_DEFAULT_REGION", this.region.as(runContext, String.class));
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

    public enum CredentialSource {
        ENVIRONMENT("Environment"),
        EC2_INSTANCE_METADATA("Ec2InstanceMetadata"),
        ECS_CONTAINER("EcsContainer");

        private final String value;

        CredentialSource(String value) {
            this.value = value;
        }
    }

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

package io.kestra.plugin.aws.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
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

import java.time.Duration;
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
    title = "Execute AWS CLI commands in a task",
    description = "Runs one or more AWS CLI statements inside the configured task runner (default Docker with amazon/aws-cli). Exports rendered AWS credentials/region to the process env, honors stsRole* fields, and sets AWS_DEFAULT_OUTPUT to outputFormat (default json). Use outputFiles to persist generated files."
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
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
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
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    commands:
                      - aws s3api list-buckets | tr -d ' \n' | xargs -0 -I {} echo '::{"outputs":{}}::'
                """
        ),
        @Example(
            full = true,
            title = "List clusters on AWS using the AWS CLI",
            code = """
                id: awscli-list-ecs-clusters
                namespace: company.team

                tasks:
                  - id: awscli
                    type: io.kestra.plugin.aws.cli.AwsCLI
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    region: us-east-1
                    outputFiles:
                      - output.json
                    commands:
                      - aws ecs list-clusters --query 'clusterArns[*]'
                      - aws ecs list-clusters > output.json
                """
        )
    }
)
public class AwsCLI extends AbstractConnection implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {
    private static final String DEFAULT_IMAGE = "amazon/aws-cli";

    @Schema(
        title = "AWS CLI commands",
        description = "Shell fragments executed in order with /bin/sh -c; include aws ... and any needed piped tooling."
    )
    @NotNull
    protected List<String> commands;

    @Schema(
        title = "Extra environment variables",
        description = "Merged into the process environment alongside AWS credentials and AWS_DEFAULT_OUTPUT."
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    protected Map<String, String> env;

    @Schema(
        title = "Deprecated Docker options",
        description = "Use taskRunner instead; retained for backward compatibility."
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "Task runner",
        description = "Runner implementation for executing the CLI; defaults to Docker runner."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "Container image",
        description = "Image used when the runner is container-based; default amazon/aws-cli."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String containerImage = DEFAULT_IMAGE;

    @Schema(
        title = "AWS CLI output format",
        description = "Sets AWS_DEFAULT_OUTPUT; default json. CLI flags still take precedence."
    )
    @PluginProperty
    @Builder.Default
    protected OutputFormat outputFormat = OutputFormat.JSON;

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    private Property<List<String>> outputFiles;

    private CredentialSource stsCredentialSource;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        List<String> allCommands = new ArrayList<>();

        // hack for missing env vars supports: https://github.com/aws/aws-cli/issues/5639
        if (this.stsRoleArn != null) {
            allCommands.add("aws configure set role_arn " + runContext.render(this.stsRoleArn).as(String.class).orElseThrow());
        }

        if (this.stsRoleSessionName != null) {
            allCommands.add("aws configure set role_session_name " + runContext.render(this.stsRoleSessionName).as(String.class).orElseThrow());
        }

        if (this.stsRoleExternalId != null) {
            allCommands.add("aws configure set external_id " + runContext.render(this.stsRoleExternalId).as(String.class).orElseThrow());
        }

        if (this.stsRoleSessionDuration != null) {
            allCommands.add("aws configure set duration_seconds " + runContext.render(stsRoleSessionDuration).as(Duration.class).orElseThrow().getSeconds());
        }

        if (this.stsCredentialSource != null) {
            allCommands.add("aws configure set credential_source " + this.stsCredentialSource.value);
        }

        allCommands.addAll(this.commands);

        var renderedOutputFiles = runContext.render(outputFiles).asList(String.class);

        CommandsWrapper commands = new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withDockerOptions(injectDefaults(getDocker()))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(new Property<>(JacksonMapper.ofJson().writeValueAsString(allCommands)))
            .withEnv(this.getEnv(runContext))
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(renderedOutputFiles.isEmpty() ? null : renderedOutputFiles);

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
            envs.put("AWS_ACCESS_KEY_ID", runContext.render(this.accessKeyId).as(String.class).orElseThrow());
        }

        if (this.secretKeyId != null) {
            envs.put("AWS_SECRET_ACCESS_KEY", runContext.render(this.secretKeyId).as(String.class).orElseThrow());
        }

        if (this.region != null) {
            envs.put("AWS_DEFAULT_REGION", runContext.render(this.region).as(String.class).orElseThrow());
        }

        if (this.sessionToken != null) {
            envs.put("AWS_SESSION_TOKEN", runContext.render(this.sessionToken).as(String.class).orElseThrow());
        }

        if (this.endpointOverride != null) {
            envs.put("AWS_ENDPOINT_URL", runContext.render(this.endpointOverride).as(String.class).orElseThrow());
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

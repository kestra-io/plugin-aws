package io.kestra.plugin.aws.cli;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class AwsCLITest extends AbstractLocalStackTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @SuppressWarnings("unchecked")
    void run() throws Exception {
        String envKey = "MY_KEY";
        String envValue = "MY_VALUE";

        AwsCLI execute = AwsCLI.builder()
            .id(IdUtils.create())
            .type(AwsCLI.class.getName())
            .docker(DockerOptions.builder()
                // needed to be able to reach localstack from inside the container
                .networkMode("host")
                .image("amazon/aws-cli")
                .entryPoint(Collections.emptyList())
                .build())
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(Property.of(localstack.getRegion()))
            .env(Map.of("{{ inputs.envKey }}", "{{ inputs.envValue }}"))
            .commands(List.of(
                "echo \"::{\\\"outputs\\\":{" +
                    "\\\"endpoint\\\":\\\"$AWS_ENDPOINT_URL\\\"," +
                    "\\\"accessKeyId\\\":\\\"$AWS_ACCESS_KEY_ID\\\"," +
                    "\\\"secretKeyId\\\":\\\"$AWS_SECRET_ACCESS_KEY\\\"," +
                    "\\\"region\\\":\\\"$AWS_DEFAULT_REGION\\\"," +
                    "\\\"format\\\":\\\"$AWS_DEFAULT_OUTPUT\\\"," +
                    "\\\"customEnv\\\":\\\"$" + envKey + "\\\"" +
                    "}}::\"",
                "aws s3 mb s3://test-bucket",
                "aws s3api list-buckets | tr -d ' \n' | xargs -0 -I {} echo '::{\"outputs\":{}}::'"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of("envKey", envKey, "envValue", envValue));

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getVars().get("endpoint"), is(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()));
        assertThat(runOutput.getVars().get("accessKeyId"), is(localstack.getAccessKey()));
        assertThat(runOutput.getVars().get("secretKeyId"), is(localstack.getSecretKey()));
        assertThat(runOutput.getVars().get("region"), is(localstack.getRegion()));
        assertThat(runOutput.getVars().get("format"), is("json"));
        assertThat(runOutput.getVars().get("customEnv"), is(envValue));
        assertThat(((Iterable<Map<String, String>>) runOutput.getVars().get("Buckets")), Matchers.allOf(
            Matchers.iterableWithSize(1),
            Matchers.hasItem(hasEntry("Name", "test-bucket"))
        ));
        assertThat(((Map<String, Object>) runOutput.getVars().get("Owner")), hasEntry("DisplayName", "webfile"));
    }
}

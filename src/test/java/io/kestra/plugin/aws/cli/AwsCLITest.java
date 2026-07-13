package io.kestra.plugin.aws.cli;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.aws.AbstractFlociTest;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.runner.docker.Docker;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

class AwsCLITest extends AbstractFlociTest {
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
            .taskRunner(
                Docker.builder()
                    .type(Docker.class.getName())
                    // needed to be able to reach localstack from inside the container
                    .networkMode("host")
                    .image("amazon/aws-cli")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .env(Map.of("{{ inputs.envKey }}", "{{ inputs.envValue }}"))
            .commands(
                Property.ofExpression(
                    JacksonMapper.ofJson().writeValueAsString(
                        List.of(
                            "echo \"::{\\\"outputs\\\":{" +
                                "\\\"endpoint\\\":\\\"$AWS_ENDPOINT_URL\\\"," +
                                "\\\"accessKeyId\\\":\\\"$AWS_ACCESS_KEY_ID\\\"," +
                                "\\\"secretKeyId\\\":\\\"$AWS_SECRET_ACCESS_KEY\\\"," +
                                "\\\"region\\\":\\\"$AWS_DEFAULT_REGION\\\"," +
                                "\\\"format\\\":\\\"$AWS_DEFAULT_OUTPUT\\\"," +
                                "\\\"customEnv\\\":\\\"$" + envKey + "\\\"" +
                                "}}::\"",
                            "aws s3 mb s3://{{ inputs.bucketName }}",
                            "echo \"::{\\\"outputs\\\":$(aws s3api list-buckets | tr -d '\\n')}::\""
                        )
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of("envKey", envKey, "envValue", envValue, "bucketName", "test-bucket"));

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getVars().get("endpoint"), is(endpointUrl()));
        assertThat(runOutput.getVars().get("accessKeyId"), is(ACCESS_KEY));
        assertThat(runOutput.getVars().get("secretKeyId"), is(SECRET_KEY));
        assertThat(runOutput.getVars().get("region"), is(REGION));
        assertThat(runOutput.getVars().get("format"), is("json"));
        assertThat(runOutput.getVars().get("customEnv"), is(envValue));
        assertThat(
            ((Iterable<Map<String, String>>) runOutput.getVars().get("Buckets")),
            Matchers.hasItem(hasEntry("Name", "test-bucket"))
        );
        assertThat(((Map<String, Object>) runOutput.getVars().get("Owner")), hasEntry("DisplayName", "owner"));
    }
}

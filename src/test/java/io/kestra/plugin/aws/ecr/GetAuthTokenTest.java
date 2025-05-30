package io.kestra.plugin.aws.ecr;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@Testcontainers
public class GetAuthTokenTest extends AbstractLocalStackTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    @Disabled
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        GetAuthToken query = GetAuthToken.builder()
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.EC2).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .build();

        GetAuthToken.TokenOutput output = query.run(runContext);
        assertThat(output, notNullValue());
    }
}

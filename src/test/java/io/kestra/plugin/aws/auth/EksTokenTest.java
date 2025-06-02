package io.kestra.plugin.aws.auth;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class EksTokenTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Disabled("To run set credentials env variables (need an EKS cluster with its region)")
    @Test
    void run() throws Exception {
        EksToken task = EksToken.builder()
            .clusterName(Property.ofValue("kestra"))
            .region(Property.ofValue("eu-west-1"))
            .build();

        EksToken.Output output = task.run(runContextFactory.of(Collections.emptyMap()));

        assertThat(output.getToken().getTokenValue(), notNullValue());
    }
}
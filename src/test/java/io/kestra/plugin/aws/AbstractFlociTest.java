package io.kestra.plugin.aws;

import org.testcontainers.junit.jupiter.Testcontainers;

import io.kestra.core.junit.annotations.KestraTest;

import io.floci.testcontainers.FlociContainer;

@KestraTest
@Testcontainers
public class AbstractFlociTest {
    protected static final String ACCESS_KEY = "test";
    protected static final String SECRET_KEY = "test";
    protected static final String REGION = "us-east-1";

    // Singleton: one container for all subclasses; Ryuk handles cleanup
    protected static final FlociContainer floci;

    static {
        floci = new FlociContainer();
        floci.start();
    }

    protected static String endpointUrl() {
        return floci.getEndpoint().toString();
    }
}

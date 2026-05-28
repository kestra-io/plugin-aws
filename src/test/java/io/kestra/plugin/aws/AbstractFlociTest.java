package io.kestra.plugin.aws;

import io.floci.testcontainers.FlociContainer;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

@KestraTest
@Testcontainers
public class AbstractFlociTest {
    protected static final String ACCESS_KEY = "test";
    protected static final String SECRET_KEY = "test";
    protected static final String REGION = "us-east-1";

    protected static FlociContainer floci;

    @BeforeAll
    static void startFloci() {
        floci = new FlociContainer();
        floci.start();
    }

    @AfterAll
    static void stopFloci() {
        if (floci != null) {
            floci.stop();
        }
    }

    protected static String endpointUrl() {
        return floci.getEndpoint().toString();
    }
}

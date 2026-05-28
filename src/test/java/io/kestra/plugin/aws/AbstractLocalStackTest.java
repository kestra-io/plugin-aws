package io.kestra.plugin.aws;

import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Volume;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@KestraTest
@Testcontainers
public class AbstractLocalStackTest {
    public static final String FLOCI_VERSION = "floci/floci:1.5.11@sha256:7bcbccf16c252f5879c2e287fcf03e94f1839de719d0fcd8222d604922cdd1ea";

    protected static final String ENDPOINT_HOST = "localhost";
    protected static final int ENDPOINT_PORT = 4566;
    protected static final String ACCESS_KEY = "test";
    protected static final String SECRET_KEY = "test";
    protected static final String REGION = "us-east-1";

    protected static GenericContainer<?> floci;

    @BeforeAll
    static void startFloci() {
        floci = new GenericContainer<>(DockerImageName.parse(FLOCI_VERSION))
            .withCreateContainerCmdModifier(cmd -> {
                HostConfig hostConfig = HostConfig.newHostConfig()
                    .withPortBindings(new PortBinding(
                        Ports.Binding.bindPort(ENDPOINT_PORT),
                        new ExposedPort(ENDPOINT_PORT)))
                    .withBinds(new Bind("/var/run/docker.sock", new Volume("/var/run/docker.sock")));
                cmd.withHostConfig(hostConfig);
            })
            .withExposedPorts(ENDPOINT_PORT)
            .waitingFor(Wait.forHttp("/").forPort(ENDPOINT_PORT).forStatusCodeMatching(code -> code < 500));
        floci.start();
    }

    @AfterAll
    static void stopFloci() {
        if (floci != null) {
            floci.stop();
        }
    }

    protected static String endpointUrl() {
        if (floci != null && floci.isRunning()) {
            return "http://" + floci.getHost() + ":" + floci.getMappedPort(ENDPOINT_PORT);
        }
        return "http://" + ENDPOINT_HOST + ":" + ENDPOINT_PORT;
    }
}

package io.kestra.plugin.aws.s3;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

@MicronautTest
@Testcontainers
abstract class AbstractTest {
    protected static LocalStackContainer localstack;

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Inject
    protected final String BUCKET = IdUtils.create().toLowerCase();

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.4.0"))
            .withServices(LocalStackContainer.Service.S3);
        // we must use fixed port in order the TriggerTest to work as it uses a real flow with hardcoded configuration
        localstack.setPortBindings(java.util.List.of("4566:4566"));
        localstack.start();
    }

    @AfterAll
    static void stopLocalstack() {
        if(localstack != null) {
            localstack.stop();
        }
    }

    protected static File file() throws URISyntaxException {
        return new File(Objects.requireNonNull(AbstractTest.class.getClassLoader()
                .getResource("application.yml"))
            .toURI());
    }

    protected String createBucket() throws Exception {
        return this.createBucket(this.BUCKET);
    }

    protected String createBucket(String bucket) throws Exception {
        CreateBucket createBucket = CreateBucket.builder()
            .id(AllTest.class.getSimpleName())
            .type(CreateBucket.class.getName())
            .bucket(bucket)
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(localstack.getRegion())
            .build();

        CreateBucket.Output createOutput = createBucket.run(runContext(createBucket));

        return createOutput.getBucket();
    }

    protected String upload(String dir) throws Exception {
        return upload(dir, this.BUCKET);
    }

    protected String upload(String dir, String bucket) throws Exception {
        URI source = storageInterface.put(
            new URI("/" + IdUtils.create()),
            new FileInputStream(file())
        );

        String out = IdUtils.create();

        Upload upload = Upload.builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .bucket(bucket)
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(localstack.getRegion())
            .from(source.toString())
            .key(dir + "/" + out + ".yml")
            .build();
        upload.run(runContext(upload));

        return upload.getKey();
    }

    protected List.ListBuilder<?, ?> list() {
        return List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .bucket(this.BUCKET)
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(localstack.getRegion());
    }

    protected RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of()
        );
    }
}

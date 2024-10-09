package io.kestra.plugin.aws.s3;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.aws.AbstractLocalStackTest;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

@KestraTest
@Testcontainers
public abstract class AbstractTest extends AbstractLocalStackTest {
    @Inject
    protected final String BUCKET = IdUtils.create().toLowerCase();
    @Inject
    protected RunContextFactory runContextFactory;
    @Inject
    protected StorageInterface storageInterface;

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
            .region(Property.of(localstack.getRegion()))
            .build();

        CreateBucket.Output createOutput = createBucket.run(runContext(createBucket));

        return createOutput.getBucket();
    }

    protected String upload(String dir) throws Exception {
        return upload(dir, this.BUCKET);
    }

    protected URI storagePut(String path) throws URISyntaxException, IOException {
        return storageInterface.put(
            null,
            new URI("/" + (path != null ? path : IdUtils.create())),
            new FileInputStream(file())
        );
    }

    protected String upload(String dir, String bucket) throws Exception {
        String out = IdUtils.create();
        URI source = storagePut(null);

        Upload upload = Upload.builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .bucket(bucket)
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(Property.of(localstack.getRegion()))
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
            .region(Property.of(localstack.getRegion()));
    }

    protected RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of()
        );
    }
}

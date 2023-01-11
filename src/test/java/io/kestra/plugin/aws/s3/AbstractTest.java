package io.kestra.plugin.aws.s3;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

@MicronautTest
abstract class AbstractTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Value("${s3.endpoint}")
    protected String endpoint;

    @Value("${s3.access-key-id}")
    protected String accessKeyId;

    @Value("${s3.secret-key-id}")
    protected String secretKeyId;

    @Value("${s3.region}")
    protected String region;

    @Inject
    protected final String BUCKET = IdUtils.create().toLowerCase();

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
            .endpointOverride(this.endpoint)
            .pathStyleAccess(true)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .region(this.region)
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
            .endpointOverride(this.endpoint)
            .pathStyleAccess(true)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .region(this.region)
            .from(source.toString())
            .key(dir + "/" + out + ".yml")
            .build();
        Upload.Output uploadOutput = upload.run(runContext(upload));

        return upload.getKey();
    }

    protected List.ListBuilder<?, ?> list() {
        return List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .bucket(this.BUCKET)
            .endpointOverride(this.endpoint)
            .pathStyleAccess(true)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .region(this.region);
    }

    protected RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of()
        );
    }
}

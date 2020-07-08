package org.kestra.task.aws.s3;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Objects;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@MicronautTest
@ExtendWith(S3MockExtension.class)
class AllTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    private final String BUCKET = FriendlyId.createFriendlyId().toLowerCase();

    @Test
    void run(final S3Client s3MockClient) throws Exception {
        CreateBucket createBucket = CreateBucket.builder()
            .id(AllTest.class.getSimpleName())
            .type(CreateBucket.class.getName())
            .bucket(this.BUCKET)
            .build();

        CreateBucket.Output createOutput = mockClient(createBucket, s3MockClient).run(runContext(createBucket));

        File file = new File(Objects.requireNonNull(AllTest.class.getClassLoader()
            .getResource("application.yml"))
            .toURI());

        URI source = storageInterface.put(
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(file)
        );

        String out = FriendlyId.createFriendlyId();

        Upload upload = Upload.builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(source.toString())
            .bucket(this.BUCKET)
            .key("tasks/aws/upload/" + out)
            .build();

        Upload.Output uploadOutput = mockClient(upload, s3MockClient).run(runContext(upload));

        Download download = Download.builder()
            .id(AllTest.class.getSimpleName())
            .type(Download.class.getName())
            .bucket(uploadOutput.getBucket())
            .key(uploadOutput.getKey())
            .build();
        Download.Output run = mockClient(download, s3MockClient).run(runContext(download));

        InputStream get = storageInterface.get(run.getUri());

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(file))))
        );

        Delete delete = Delete.builder()
            .id(AllTest.class.getSimpleName())
            .type(Delete.class.getName())
            .bucket(uploadOutput.getBucket())
            .key(uploadOutput.getKey())
            .build();
        Delete.Output deleteOutput = mockClient(delete, s3MockClient).run(runContext(delete));

        assertThrows(
            NoSuchKeyException.class,
            () -> mockClient(download, s3MockClient).run(runContext(download))
        );
    }

    private static <T extends AbstractS3> T mockClient(T s3, S3Client s3MockClient) throws IllegalVariableEvaluationException {
        T spy = spy(s3);

        doReturn(s3MockClient)
            .when(spy)
            .client(any());

        return spy;
    }

    private RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of()
        );
    }
}

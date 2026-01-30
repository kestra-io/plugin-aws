package io.kestra.plugin.aws.s3;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

class DownloadsTest extends AbstractTest {
    @Test
    void delete() throws Exception {
        this.createBucket();

        upload("/tasks/s3");
        upload("/tasks/s3");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .action(Property.ofValue(ActionInterface.Action.DELETE))
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(2));
        assertThat(run.getObjects().get(0).getUri().toString(), endsWith(".yml"));
        assertThat(run.getOutputFiles().size(), is(2));

        List list = list().build();
        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(0));
    }

    @Test
    void move() throws Exception {
        this.createBucket();

        upload("/tasks/s3-from");
        upload("/tasks/s3-from");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(Property.ofExpression("{{bucket}}"))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveTo(Copy.CopyObject.builder()
                .key(Property.ofValue("/tasks/s3-move"))
                .build()
            )
            .build();

        Downloads.Output run = task.run(runContextFactory.of(Map.of("bucket", this.BUCKET)));

        assertThat(run.getObjects().size(), is(2));
        assertThat(run.getOutputFiles().size(), is(2));

        List list = list().prefix(Property.ofValue("/tasks/s3-from")).build();
        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(0));

        list = list().prefix(Property.ofValue("/tasks/s3-move")).build();
        listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(2));
    }

    @Test
    void maxFilesExceeded() throws Exception {
        this.createBucket();

        // Upload 5 files
        for (int i = 0; i < 5; i++) {
            upload("/tasks/s3-maxfiles");
        }

        // Downloads with maxFiles=3 (less than 5 files) - should return first 3 files (truncated)
        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .prefix(Property.ofValue("/tasks/s3-maxfiles"))
            .maxFiles(Property.ofValue(3))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(3));
    }

    @Test
    void maxFilesNotExceeded() throws Exception {
        this.createBucket();

        // Upload 5 files
        for (int i = 0; i < 5; i++) {
            upload("/tasks/s3-maxfiles-ok");
        }

        // Downloads with maxFiles=10 (more than 5 files) - should return all 5 files
        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .prefix(Property.ofValue("/tasks/s3-maxfiles-ok"))
            .maxFiles(Property.ofValue(10))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(5));
    }
}

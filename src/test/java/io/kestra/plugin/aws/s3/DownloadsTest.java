package io.kestra.plugin.aws.s3;

import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

class DownloadsTest extends AbstractTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void delete() throws Exception {
        this.createBucket();

        upload("/tasks/s3");
        upload("/tasks/s3");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(this.BUCKET)
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(localstack.getRegion())
            .action(ActionInterface.Action.DELETE)
            .build();

        List.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(2));
        assertThat(run.getObjects().get(0).getUri().toString(), endsWith(".yml"));

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
            .bucket("{{bucket}}")
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(localstack.getRegion())
            .action(ActionInterface.Action.MOVE)
            .moveTo(Copy.CopyObject.builder()
                .key("/tasks/s3-move")
                .build()
            )
            .build();

        List.Output run = task.run(runContextFactory.of(Map.of("bucket", this.BUCKET)));

        assertThat(run.getObjects().size(), is(2));

        List list = list().prefix("/tasks/s3-from").build();
        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(0));

        list = list().prefix("/tasks/s3-move").build();
        listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(2));
    }
}

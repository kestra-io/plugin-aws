package io.kestra.plugin.aws.s3;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
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
            .bucket(this.BUCKET)
            .endpointOverride(this.endpoint)
            .pathStyleAccess(true)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .region(this.region)
            .action(ActionInterface.Action.DELETE)
            .build();

        List.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(2));

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
            .bucket(this.BUCKET)
            .endpointOverride(this.endpoint)
            .pathStyleAccess(true)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .action(ActionInterface.Action.MOVE)
            .region(this.region)
            .moveTo(Copy.CopyObject.builder()
                .key("/tasks/s3-move")
                .build()
            )
            .build();

        List.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(2));

        List list = list().prefix("/tasks/s3-from").build();
        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(0));

        list = list().prefix("/tasks/s3-move").build();
        listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(2));
    }
}

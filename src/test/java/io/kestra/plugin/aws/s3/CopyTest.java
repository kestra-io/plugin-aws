package io.kestra.plugin.aws.s3;

import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class CopyTest extends AbstractTest {
    void run(Boolean delete) throws Exception {
        this.createBucket();

        String upload = upload("/tasks/s3/" + IdUtils.create() + "/sub");
        String move = upload("/tasks/s3/" + IdUtils.create() + "/sub");

        // copy
        Copy task = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(List.class.getName())
            .endpointOverride(this.endpoint)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .region(this.region)
            .from(Copy.CopyObjectFrom.builder()
                .bucket(this.BUCKET)
                .key(upload)
                .build()
            )
            .to(Copy.CopyObject.builder()
                .key(move)
                .build()
            )
            .delete(delete)
            .build();

        Copy.Output run = task.run(runContext(task));
        assertThat(run.getKey(), is(move));

        // list
        List list = list().prefix(move).build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(1));

        // original is here
        list = list().prefix(upload).build();

        listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(delete ? 0 : 1));
    }

    @Test
    void run() throws Exception {
        this.run(false);
    }

    @Test
    void delete() throws Exception {
        this.run(true);
    }
}

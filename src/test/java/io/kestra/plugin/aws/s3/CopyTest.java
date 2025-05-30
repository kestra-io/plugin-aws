package io.kestra.plugin.aws.s3;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

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
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .from(Copy.CopyObjectFrom.builder()
                .bucket(Property.ofValue(this.BUCKET))
                .key(Property.ofValue(upload))
                .build()
            )
            .to(Copy.CopyObject.builder()
                .key(Property.ofValue(move))
                .build()
            )
            .delete(Property.ofValue(delete))
            .build();

        Copy.Output run = task.run(runContext(task));
        assertThat(run.getKey(), is(move));

        // list
        List list = list().prefix(Property.ofValue(move)).build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(1));

        // original is here
        list = list().prefix(Property.ofValue(upload)).build();

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

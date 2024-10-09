package io.kestra.plugin.aws.s3;

import io.kestra.core.models.property.Property;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

class DeleteListTest extends AbstractTest {
    @Test
    void run() throws Exception {
        this.createBucket();

        for (int i = 0; i < 10; i++) {
            upload("/tasks/s3");
        }

        // all listing
        DeleteList task = DeleteList.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .bucket(this.BUCKET)
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(Property.of(localstack.getRegion()))
            .concurrent(5)
            .build();
        DeleteList.Output run = task.run(runContext(task));
        assertThat(run.getCount(), is(10L));
        assertThat(run.getSize(), greaterThan(1000L));
    }
}

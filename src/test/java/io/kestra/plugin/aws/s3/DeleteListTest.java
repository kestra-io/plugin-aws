package io.kestra.plugin.aws.s3;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;

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
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .concurrent(5)
            .build();
        DeleteList.Output run = task.run(runContext(task));
        assertThat(run.getCount(), is(10L));
        assertThat(run.getSize(), greaterThan(1000L));
    }
}

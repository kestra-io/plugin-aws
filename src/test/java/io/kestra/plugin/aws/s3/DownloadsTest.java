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
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .action(Property.of(ActionInterface.Action.DELETE))
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
            .bucket(new Property<>("{{bucket}}"))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .action(Property.of(ActionInterface.Action.MOVE))
            .moveTo(Copy.CopyObject.builder()
                .key(Property.of("/tasks/s3-move"))
                .build()
            )
            .build();

        Downloads.Output run = task.run(runContextFactory.of(Map.of("bucket", this.BUCKET)));

        assertThat(run.getObjects().size(), is(2));
        assertThat(run.getOutputFiles().size(), is(2));

        List list = list().prefix(Property.of("/tasks/s3-from")).build();
        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(0));

        list = list().prefix(Property.of("/tasks/s3-move")).build();
        listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(2));
    }
}

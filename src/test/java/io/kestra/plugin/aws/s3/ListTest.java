package io.kestra.plugin.aws.s3;

import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ListTest extends AbstractTest {
    @Test
    void emptyFolderAlwaysSerialized() throws Exception {
        this.createBucket();

        List task = list()
            .build();
        List.Output run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(0));

        assertThat(JacksonMapper.ofJson().writeValueAsString(run), is("{\"objects\":[]}"));
    }

    @Test
    void run() throws Exception {
        this.createBucket();

        String dir = IdUtils.create();
        String lastFileName = null;

        for (int i = 0; i < 10; i++) {
            lastFileName = upload("/tasks/s3/" + dir);
        }
        upload("/tasks/s3/" + dir + "/sub");

        // all listing
        List task = list()
            .build();
        List.Output run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(11));

        // Dir listing
        task = list()
            .filter(Property.ofValue(ListInterface.Filter.FILES))
            .prefix(Property.ofValue("/tasks/s3/" + dir + "/sub"))
            .build();
        run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(1));

        // prefix
        task = list()
            .prefix(Property.ofValue("/tasks/s3/" + dir + "/sub"))
            .build();
        run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(1));

        // regexp
        task = list()
            .regexp(Property.ofValue("/tasks/s3/.*/" + StringUtils.substringAfterLast(lastFileName, "/")))
            .build();
        run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(1));
    }

    @Test
    void maxFilesExceeded() throws Exception {
        this.createBucket();

        String dir = IdUtils.create();

        // Upload 5 files
        for (int i = 0; i < 5; i++) {
            upload("/tasks/s3/" + dir);
        }

        // List with maxFiles=3 (less than 5 files) - should return empty list
        List task = list()
            .prefix(Property.ofValue("/tasks/s3/" + dir))
            .maxFiles(Property.ofValue(3))
            .build();
        List.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(0));
    }

    @Test
    void maxFilesNotExceeded() throws Exception {
        this.createBucket();

        String dir = IdUtils.create();

        // Upload 5 files
        for (int i = 0; i < 5; i++) {
            upload("/tasks/s3/" + dir);
        }

        // List with maxFiles=10 (more than 5 files) - should return all 5 files
        List task = list()
            .prefix(Property.ofValue("/tasks/s3/" + dir))
            .maxFiles(Property.ofValue(10))
            .build();
        List.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(5));
    }

    @Test
    void maxFilesDefault() throws Exception {
        this.createBucket();

        String dir = IdUtils.create();

        // Upload 30 files (more than default limit of 25)
        for (int i = 0; i < 30; i++) {
            upload("/tasks/s3/" + dir);
        }

        // List WITHOUT specifying maxFiles - should use default of 25 and return empty
        List task = list()
            .prefix(Property.ofValue("/tasks/s3/" + dir))
            .build();
        List.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(0));
    }
}

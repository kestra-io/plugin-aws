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
            .filter(Property.of(ListInterface.Filter.FILES))
            .prefix(Property.of("/tasks/s3/" + dir + "/sub"))
            .build();
        run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(1));

        // prefix
        task = list()
            .prefix(Property.of("/tasks/s3/" + dir + "/sub"))
            .build();
        run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(1));

        // regexp
        task = list()
            .regexp(Property.of("/tasks/s3/.*/" + StringUtils.substringAfterLast(lastFileName, "/")))
            .build();
        run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(1));
    }
}

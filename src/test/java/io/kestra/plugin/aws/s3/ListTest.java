package io.kestra.plugin.aws.s3;

import io.kestra.core.utils.IdUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class ListTest extends AbstractTest {
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
            .filter(ListInterface.Filter.FILES)
            .prefix("/tasks/s3/" + dir + "/sub")
            .build();
        run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(1));

        // prefix
        task = list()
            .prefix("/tasks/s3/" + dir + "/sub")
            .build();
        run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(1));

        // regexp
        task = list()
            .regexp("tasks/s3/.*/" + StringUtils.substringAfterLast(lastFileName, "/"))
            .build();
        run = task.run(runContext(task));
        assertThat(run.getObjects().size(), is(1));
    }
}

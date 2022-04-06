package io.kestra.plugin.aws.s3;

import com.google.common.io.CharStreams;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AllTest extends AbstractTest{
    @Test
    void run() throws Exception {
        this.createBucket();

        String key = upload("tasks/aws/upload");

        // list
        List list = List.builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .bucket(this.BUCKET)
            .endpointOverride(this.endpoint)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .region(this.region)
            .prefix("tasks/aws/upload/")
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(1));

        // download
        Download download = Download.builder()
            .id(AllTest.class.getSimpleName())
            .type(Download.class.getName())
            .bucket(this.BUCKET)
            .endpointOverride(this.endpoint)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .region(this.region)
            .key(key)
            .build();
        Download.Output run = download.run(runContext(download));

        InputStream get = storageInterface.get(run.getUri());
        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(file()))))
        );

        // delete
        Delete delete = Delete.builder()
            .id(AllTest.class.getSimpleName())
            .type(Delete.class.getName())
            .bucket(this.BUCKET)
            .endpointOverride(this.endpoint)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .region(this.region)
            .key(key)
            .build();
        Delete.Output deleteOutput = delete.run(runContext(delete));
        assertThat(deleteOutput.getDeleteMarker(), is(nullValue()));

        // delete missing
        assertThrows(
            NoSuchKeyException.class,
            () -> download.run(runContext(download))
        );
    }
}

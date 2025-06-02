package io.kestra.plugin.aws.s3;

import com.google.common.io.CharStreams;
import io.kestra.core.models.property.Property;
import io.kestra.core.tenant.TenantService;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .prefix(Property.of("tasks/aws/upload/"))
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(1));

        // download
        Download download = Download.builder()
            .id(AllTest.class.getSimpleName())
            .type(Download.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .key(Property.of(key))
            .build();
        Download.Output run = download.run(runContext(download));

        InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, run.getUri());
        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(file()))))
        );

        // delete
        Delete delete = Delete.builder()
            .id(AllTest.class.getSimpleName())
            .type(Delete.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .key(Property.of(key))
            .build();
        Delete.Output deleteOutput = delete.run(runContext(delete));
        assertThat(deleteOutput.getDeleteMarker(), is(nullValue()));

        // delete missing
        ExecutionException exp = assertThrows(
            ExecutionException.class,
            () -> download.run(runContext(download))
        );
        assertThat(exp.getCause(), instanceOf(S3Exception.class));
        S3Exception cause = (S3Exception) exp.getCause();
        assertThat(cause.statusCode(), is(404));
    }
}

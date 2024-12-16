package io.kestra.plugin.aws.s3;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class UploadsTest extends AbstractTest{
    @Test
    void run() throws Exception {
        this.createBucket();

        URI source1 = storagePut("1.yml");
        URI source2 = storagePut("2.yml");
        URI source3 = storagePut("3.yml");
        URI source4 = storagePut("4.yml");

        // List of string
        Upload upload = Upload.builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .from(java.util.List.of(source1.toString(), source2.toString(), source3.toString(), source4.toString()))
            .key(Property.of(IdUtils.create() + "/"))
            .build();
        upload.run(runContext(upload));

        // list
        List list = List.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(Upload.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .prefix(upload.getKey())
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(4));
        assertThat(listOutput.getObjects().stream().filter(s3Object -> s3Object.getKey().contains("1.yml")).count(), is(1L));
    }
}

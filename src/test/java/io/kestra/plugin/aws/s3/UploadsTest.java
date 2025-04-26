package io.kestra.plugin.aws.s3;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.s3.models.S3Object;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UploadsTest extends AbstractTest {
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


    @Test
    void run_singleString() throws Exception {
        this.createBucket();

        URI source = storagePut("single.yml");

        Upload upload = Upload.builder()
            .id("SingleStringTest")
            .type(Upload.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .from(source.toString())
            .key(Property.of(IdUtils.create() + "/single.yml"))
            .build();
        upload.run(runContext(upload));

        List list = List.builder()
            .id("SingleStringListTest")
            .type(Upload.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .prefix(upload.getKey())
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(1));
        assertThat(listOutput.getObjects().getFirst().getKey().contains("single.yml"), is(true));
    }

    @Test
    void run_jsonArrayString() throws Exception {
        this.createBucket();

        URI source1 = storagePut("1.yml");
        URI source2 = storagePut("2.yml");

        String jsonArray = "[\n  \"" + source1.toString() + "\",\n  \"" + source2.toString() + "\"\n]\n";

        Upload upload = Upload.builder()
            .id("JsonArrayStringTest")
            .type(Upload.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .from(jsonArray)
            .key(Property.of(IdUtils.create() + "/"))
            .build();
        upload.run(runContext(upload));

        List list = List.builder()
            .id("JsonArrayStringListTest")
            .type(Upload.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .prefix(upload.getKey())
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(2));
        assertThat(listOutput.getObjects().stream().map(S3Object::getKey).allMatch(key -> key.contains("1.yml") || key.contains("2.yml")), is(true));
    }

    @Test
    void run_emptyJsonArray() throws Exception {
        this.createBucket();

        String emptyJsonArray = "[]";

        Upload upload = Upload.builder()
            .id("EmptyJsonArrayTest")
            .type(Upload.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpointOverride(Property.of(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.of(localstack.getAccessKey()))
            .secretKeyId(Property.of(localstack.getSecretKey()))
            .region(Property.of(localstack.getRegion()))
            .from(emptyJsonArray)
            .key(Property.of(IdUtils.create() + "/"))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> upload.run(runContext(upload))
        );

        assertThat(exception.getMessage(), is("No files to upload: empty array provided in 'from' property"));
    }
}

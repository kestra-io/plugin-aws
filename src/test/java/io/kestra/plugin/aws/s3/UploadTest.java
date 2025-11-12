package io.kestra.plugin.aws.s3;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.s3.models.S3Object;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UploadTest extends AbstractTest {
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
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .from(java.util.List.of(source1.toString(), source2.toString(), source3.toString(), source4.toString()))
            .key(Property.ofValue(IdUtils.create() + "/"))
            .build();

        Upload.Output uploadOutput = upload.run(runContext(upload));

        // Verify MultiFileUploadOutput
        assertThat(uploadOutput.getBucket(), is(this.BUCKET));
        assertThat(uploadOutput.getKey(), is(notNullValue()));
        assertThat(uploadOutput.getFiles(), is(notNullValue()));
        assertThat(uploadOutput.getFiles().size(), is(4));
        assertThat(uploadOutput.getFiles().keySet(), hasItems("1.yml", "2.yml", "3.yml", "4.yml"));

        // list
        List list = List.builder()
            .id(UploadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
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
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .from(source.toString())
            .key(Property.ofValue(IdUtils.create() + "/single.yml"))
            .build();

        Upload.Output uploadOutput = upload.run(runContext(upload));

        // Verify Upload.Output
        assertThat(uploadOutput.getBucket(), is(this.BUCKET));
        assertThat(uploadOutput.getKey(), is(notNullValue()));
        assertThat(uploadOutput.getETag(), is(notNullValue()));

        List list = List.builder()
            .id("SingleStringListTest")
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
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

        String jsonArray = "\n[\n  \"" + source1.toString() + "\",\n  \"" + source2.toString() + "\"\n]\n";

        Upload upload = Upload.builder()
            .id("JsonArrayStringTest")
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .from(jsonArray)
            .key(Property.ofValue(IdUtils.create() + "/"))
            .build();

        Upload.Output uploadOutput = upload.run(runContext(upload));

        assertThat(uploadOutput.getBucket(), is(this.BUCKET));
        assertThat(uploadOutput.getKey(), is(notNullValue()));
        assertThat(uploadOutput.getFiles(), is(notNullValue()));
        assertThat(uploadOutput.getFiles().size(), is(2));
        assertThat(uploadOutput.getFiles().keySet(), hasItems("1.yml", "2.yml"));

        List list = List.builder()
            .id("JsonArrayStringListTest")
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
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
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .from(emptyJsonArray)
            .key(Property.ofValue(IdUtils.create() + "/"))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> upload.run(runContext(upload))
        );

        assertThat(exception.getMessage(), is("No files to upload: the 'from' property contains an empty collection or array"));
    }

    @Test
    void run_emptyCollection() throws Exception {
        this.createBucket();

        Upload upload = Upload.builder()
            .id("EmptyCollectionTest")
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .from(java.util.Collections.emptyList())
            .key(Property.ofValue(IdUtils.create() + "/"))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> upload.run(runContext(upload))
        );

        assertThat(exception.getMessage(), is("No files to upload: the 'from' property contains an empty collection or array"));
    }

    @Test
    void run_mapWithCustomKeys() throws Exception {
        this.createBucket();

        URI source1 = storagePut("file1.yml");
        URI source2 = storagePut("file2.yml");
        URI source3 = storagePut("file3.yml");

        // Map with custom S3 keys
        java.util.Map<String, String> filesMap = new java.util.HashMap<>();
        filesMap.put("custom/path/renamed1.yml", source1.toString());
        filesMap.put("another/location/renamed2.yml", source2.toString());
        filesMap.put("deep/nested/path/renamed3.yml", source3.toString());

        String baseKey = IdUtils.create() + "/";

        Upload upload = Upload.builder()
            .id("MapWithCustomKeysTest")
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .from(filesMap)
            .key(Property.ofValue(baseKey))
            .build();

        Upload.Output uploadOutput = upload.run(runContext(upload));

        // Verify output
        assertThat(uploadOutput.getBucket(), is(this.BUCKET));
        assertThat(uploadOutput.getKey(), is(baseKey));
        assertThat(uploadOutput.getFiles(), is(notNullValue()));
        assertThat(uploadOutput.getFiles().size(), is(3));
        assertThat(uploadOutput.getFiles().keySet(), hasItems(
            "custom/path/renamed1.yml",
            "another/location/renamed2.yml",
            "deep/nested/path/renamed3.yml"
        ));

        // Verify files were uploaded to correct locations
        List list = List.builder()
            .id("MapWithCustomKeysListTest")
            .type(List.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .prefix(Property.ofValue(baseKey))
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(3));
        assertThat(listOutput.getObjects().stream()
            .anyMatch(obj -> obj.getKey().endsWith("custom/path/renamed1.yml")), is(true));
        assertThat(listOutput.getObjects().stream()
            .anyMatch(obj -> obj.getKey().endsWith("another/location/renamed2.yml")), is(true));
        assertThat(listOutput.getObjects().stream()
            .anyMatch(obj -> obj.getKey().endsWith("deep/nested/path/renamed3.yml")), is(true));
    }

    @Test
    void run_jsonMapString() throws Exception {
        this.createBucket();

        URI source1 = storagePut("file1.yml");
        URI source2 = storagePut("file2.yml");

        // JSON map string with custom keys
        String jsonMap = String.format(
            "{\n  \"reports/2024/report1.yml\": \"%s\",\n  \"reports/2024/report2.yml\": \"%s\"\n}",
            source1.toString(),
            source2.toString()
        );

        String baseKey = IdUtils.create() + "/";

        Upload upload = Upload.builder()
            .id("JsonMapStringTest")
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .from(jsonMap)
            .key(Property.ofValue(baseKey))
            .build();

        Upload.Output uploadOutput = upload.run(runContext(upload));

        assertThat(uploadOutput.getBucket(), is(this.BUCKET));
        assertThat(uploadOutput.getKey(), is(baseKey));
        assertThat(uploadOutput.getFiles(), is(notNullValue()));
        assertThat(uploadOutput.getFiles().size(), is(2));
        assertThat(uploadOutput.getFiles().keySet(), hasItems(
            "reports/2024/report1.yml",
            "reports/2024/report2.yml"
        ));

        // Verify files in S3
        List list = List.builder()
            .id("JsonMapStringListTest")
            .type(List.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .prefix(Property.ofValue(baseKey))
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(2));
    }

    @Test
    void run_emptyMap() throws Exception {
        this.createBucket();

        Upload upload = Upload.builder()
            .id("EmptyMapTest")
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .from(java.util.Collections.emptyMap())
            .key(Property.ofValue(IdUtils.create() + "/"))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> upload.run(runContext(upload))
        );

        assertThat(exception.getMessage(), is("No files to upload: the 'from' property contains an empty collection or array"));
    }
}
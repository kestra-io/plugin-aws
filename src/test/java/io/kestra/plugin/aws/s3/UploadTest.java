package io.kestra.plugin.aws.s3;

import java.net.URI;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.aws.s3.models.S3Object;

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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(filesMap)
            .key(Property.ofValue(baseKey))
            .build();

        Upload.Output uploadOutput = upload.run(runContext(upload));

        // Verify output
        assertThat(uploadOutput.getBucket(), is(this.BUCKET));
        assertThat(uploadOutput.getKey(), is(baseKey));
        assertThat(uploadOutput.getFiles(), is(notNullValue()));
        assertThat(uploadOutput.getFiles().size(), is(3));
        assertThat(
            uploadOutput.getFiles().keySet(), hasItems(
                "custom/path/renamed1.yml",
                "another/location/renamed2.yml",
                "deep/nested/path/renamed3.yml"
            )
        );

        // Verify files were uploaded to correct locations
        List list = List.builder()
            .id("MapWithCustomKeysListTest")
            .type(List.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .prefix(Property.ofValue(baseKey))
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(3));
        assertThat(
            listOutput.getObjects().stream()
                .anyMatch(obj -> obj.getKey().endsWith("custom/path/renamed1.yml")),
            is(true)
        );
        assertThat(
            listOutput.getObjects().stream()
                .anyMatch(obj -> obj.getKey().endsWith("another/location/renamed2.yml")),
            is(true)
        );
        assertThat(
            listOutput.getObjects().stream()
                .anyMatch(obj -> obj.getKey().endsWith("deep/nested/path/renamed3.yml")),
            is(true)
        );
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(jsonMap)
            .key(Property.ofValue(baseKey))
            .build();

        Upload.Output uploadOutput = upload.run(runContext(upload));

        assertThat(uploadOutput.getBucket(), is(this.BUCKET));
        assertThat(uploadOutput.getKey(), is(baseKey));
        assertThat(uploadOutput.getFiles(), is(notNullValue()));
        assertThat(uploadOutput.getFiles().size(), is(2));
        assertThat(
            uploadOutput.getFiles().keySet(), hasItems(
                "reports/2024/report1.yml",
                "reports/2024/report2.yml"
            )
        );

        // Verify files in S3
        List list = List.builder()
            .id("JsonMapStringListTest")
            .type(List.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
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
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(java.util.Collections.emptyMap())
            .key(Property.ofValue(IdUtils.create() + "/"))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> upload.run(runContext(upload))
        );

        assertThat(exception.getMessage(), is("No files to upload: the 'from' property contains an empty collection or array"));
    }

    @Test
    void run_singleKestraUriShouldNotBeParsedAsJson() throws Exception {
        this.createBucket();

        URI source = storagePut("manifest.json");

        Upload upload = Upload.builder()
            .id("SingleKestraUriJsonTest")
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(source.toString())
            .key(Property.ofValue(IdUtils.create() + "/manifest.json"))
            .build();

        Upload.Output uploadOutput = upload.run(runContext(upload));

        assertThat(uploadOutput.getBucket(), is(this.BUCKET));
        assertThat(uploadOutput.getKey(), containsString("manifest.json"));
        assertThat(uploadOutput.getETag(), is(notNullValue()));

        List list = List.builder()
            .id("SingleKestraUriJsonListTest")
            .type(List.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .prefix(upload.getKey())
            .build();

        List.Output listOutput = list.run(runContext(list));

        assertThat(listOutput.getObjects().size(), is(1));
        assertThat(listOutput.getObjects().getFirst().getKey(), containsString("manifest.json"));
    }

    @Test
    void run_listOfExpressions() throws Exception {
        this.createBucket();

        URI source1 = storagePut("first.yml");
        URI source2 = storagePut("second.yml");

        Upload upload = Upload.builder()
            .id("ListOfExpressionsTest")
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(java.util.List.of("{{ inputs.file1 }}", "{{ inputs.file2 }}"))
            .key(Property.ofValue(IdUtils.create() + "/"))
            .build();

        var runContext = TestsUtils.mockRunContext(
            this.runContextFactory,
            upload,
            Map.of("file1", source1.toString(), "file2", source2.toString())
        );

        Upload.Output uploadOutput = upload.run(runContext);

        assertThat(uploadOutput.getFiles().keySet(), containsInAnyOrder("first.yml", "second.yml"));
    }

    @Test
    void run_collidingFileNamesInListShouldThrow() throws Exception {
        this.createBucket();

        URI source1 = storagePut("report.csv");
        URI source2 = storagePut("report.csv");

        Upload upload = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(java.util.List.of(source1.toString(), source2.toString()))
            .key(Property.ofValue(IdUtils.create() + "/"))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> upload.run(runContext(upload))
        );

        assertThat(
            exception.getMessage(),
            is("Cannot upload files: multiple source URIs resolve to the same destination key 'report.csv': '%s' and '%s'. Use a Map in the 'from' property to specify unique destination keys for each file, or ensure source file names are distinct.".formatted(source1, source2))
        );
    }

    @Test
    void shouldThrowWhenUriListContainsCollidingFileNames() {
        var uris = java.util.List.of(
            "kestra:///step1/report.csv",
            "kestra:///step2/report.csv"
        );

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> Upload.uriListToMap(uris)
        );

        assertThat(
            exception.getMessage(),
            is("Cannot upload files: multiple source URIs resolve to the same destination key 'report.csv': 'kestra:///step1/report.csv' and 'kestra:///step2/report.csv'. Use a Map in the 'from' property to specify unique destination keys for each file, or ensure source file names are distinct.")
        );
    }
}

package io.kestra.plugin.aws.s3;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DownloadTest extends AbstractTest {

    @Test
    void runSingleFileMode() throws Exception {
        this.createBucket();

        // Upload a file to S3 to test download
        String key = IdUtils.create() + "/test.yml";
        URI sourceFile = storagePut("test.yml");

        // Upload the file to S3
        uploadFile(sourceFile, key);

        // Test downloading a single file
        Download download = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .key(Property.ofValue(key))
            .build();

        Download.Output output = download.run(runContext(download));

        // Assert single file download results
        assertThat(output.getContentLength(), notNullValue());
        assertThat(output.getContentType(), notNullValue());
        assertThat(output.getUri().toString(), notNullValue());
        assertThat(output.getMetadata().size(), notNullValue());
    }

    @Test
    void runWithPrefixesAndFilters() throws Exception {
        this.createBucket();

        // Create a hierarchical structure with multiple folders
        String basePrefix = IdUtils.create() + "/";
        String folder1 = basePrefix + "folder1/";
        String folder1Sub1 = folder1 + "subfolder1/";
        String folder1Sub2 = folder1 + "subfolder2/";
        String folder2 = basePrefix + "folder2/";

        // Create test files in different locations in the hierarchy
        URI fileA1 = storagePut("a1.txt");
        URI fileA2 = storagePut("a2.txt");
        URI fileB1 = storagePut("b1.json");
        URI fileB2 = storagePut("b2.json");
        URI fileC1 = storagePut("c1.yaml");

        // Upload files to different locations in the S3 bucket hierarchy
        uploadFile(fileA1, folder1Sub1 + "a1.txt");
        uploadFile(fileA2, folder1Sub2 + "a2.txt");
        uploadFile(fileB1, folder1 + "b1.json");
        uploadFile(fileB2, folder2 + "b2.json");
        uploadFile(fileC1, folder2 + "c1.yaml");

        // Test 1: Download only from folder1 with all subfolders
        Download downloadFolder1 = Download.builder()
            .id(DownloadTest.class.getSimpleName() + "-folder1")
            .type(Download.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .prefix(Property.ofValue(folder1))
            .build();

        Download.Output outputFolder1 = downloadFolder1.run(runContext(downloadFolder1));
        assertThat(outputFolder1.getFiles().size(), is(3));

        assertThat(outputFolder1.getFiles().keySet(), hasItems(
            containsString("a1.txt"),
            containsString("a2.txt"),
            containsString("b1.json")
        ));

        // Test 2: Download only from folder1 with delimiter (only direct files in folder1)
        Download downloadFolder1Direct = Download.builder()
            .id(DownloadTest.class.getSimpleName() + "-folder1Direct")
            .type(Download.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .prefix(Property.ofValue(folder1))
            .delimiter(Property.ofValue("/"))
            .build();

        Download.Output outputFolder1Direct = downloadFolder1Direct.run(runContext(downloadFolder1Direct));
        assertThat(outputFolder1Direct.getFiles().size(), is(1));
        assertThat(outputFolder1Direct.getFiles().keySet(), hasItems(
            containsString("b1.json")
        ));

        // Test 3: Use regexp to filter only .txt files from the entire hierarchy
        Download downloadTxtFiles = Download.builder()
            .id(DownloadTest.class.getSimpleName() + "-txtFiles")
            .type(Download.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .prefix(Property.ofValue(basePrefix))
            .regexp(Property.ofValue(".*\\.txt$"))
            .build();

        Download.Output outputTxtFiles = downloadTxtFiles.run(runContext(downloadTxtFiles));
        assertThat(outputTxtFiles.getFiles().size(), is(2));

        assertThat(outputTxtFiles.getFiles().keySet(), hasItems(
            containsString("a1.txt"),
            containsString("a2.txt")
        ));
    }

    private void uploadFile(URI source, String key) throws Exception {
        Upload upload = Upload.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .from(source.toString())
            .key(Property.ofValue(key))
            .build();
        upload.run(runContext(upload));
    }

    @Test
    void testInvalidConfiguration() throws Exception {
        this.createBucket();

        Download invalidDownload = Download.builder()
            .id(DownloadTest.class.getSimpleName() + "-invalid")
            .type(Download.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .build();


        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> invalidDownload.run(runContext(invalidDownload))
        );

        assertThat(
            exception.getMessage(),
            containsString("Invalid configuration: either specify 'key' for single file download or at least one filtering parameter")
        );
    }

    @Test
    void maxFilesExceeded() throws Exception {
        this.createBucket();

        String basePrefix = IdUtils.create() + "/maxfiles-test/";

        // Upload 5 files
        for (int i = 0; i < 5; i++) {
            URI file = storagePut("file" + i + ".txt");
            uploadFile(file, basePrefix + "file" + i + ".txt");
        }

        // Download with maxFiles=3 (less than 5 files) - should return empty/null files
        Download download = Download.builder()
            .id(DownloadTest.class.getSimpleName() + "-maxFilesExceeded")
            .type(Download.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .prefix(Property.ofValue(basePrefix))
            .maxFiles(Property.ofValue(3))
            .build();

        Download.Output output = download.run(runContext(download));

        // When maxFiles exceeded, List returns empty, so Download gets no files
        assertThat(output.getFiles(), anyOf(nullValue(), anEmptyMap()));
    }

    @Test
    void maxFilesNotExceeded() throws Exception {
        this.createBucket();

        String basePrefix = IdUtils.create() + "/maxfiles-ok/";

        // Upload 5 files
        for (int i = 0; i < 5; i++) {
            URI file = storagePut("file" + i + ".txt");
            uploadFile(file, basePrefix + "file" + i + ".txt");
        }

        // Download with maxFiles=10 (more than 5 files) - should return all 5 files
        Download download = Download.builder()
            .id(DownloadTest.class.getSimpleName() + "-maxFilesNotExceeded")
            .type(Download.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
            .accessKeyId(Property.ofValue(localstack.getAccessKey()))
            .secretKeyId(Property.ofValue(localstack.getSecretKey()))
            .region(Property.ofValue(localstack.getRegion()))
            .prefix(Property.ofValue(basePrefix))
            .maxFiles(Property.ofValue(10))
            .build();

        Download.Output output = download.run(runContext(download));

        assertThat(output.getFiles().size(), is(5));
    }
}
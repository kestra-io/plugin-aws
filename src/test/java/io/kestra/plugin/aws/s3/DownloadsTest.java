package io.kestra.plugin.aws.s3;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.s3.models.S3Object;

import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DownloadsTest extends AbstractTest {
    @Test
    void delete() throws Exception {
        this.createBucket();

        upload("/tasks/s3");
        upload("/tasks/s3");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .action(Property.ofValue(ActionInterface.Action.DELETE))
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(2));
        assertThat(run.getObjects().getFirst().getUri().toString(), endsWith(".yml"));
        assertThat(run.getOutputFiles().size(), is(2));

        List list = list().build();
        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(0));
    }

    @Test
    void move() throws Exception {
        this.createBucket();

        upload("/tasks/s3-from");
        upload("/tasks/s3-from");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(Property.ofExpression("{{bucket}}"))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveTo(
                Copy.CopyObject.builder()
                    .key(Property.ofValue("/tasks/s3-move"))
                    .build()
            )
            .build();

        Downloads.Output run = task.run(runContextFactory.of(Map.of("bucket", this.BUCKET)));

        assertThat(run.getObjects().size(), is(2));
        assertThat(run.getOutputFiles().size(), is(2));

        List list = list().prefix(Property.ofValue("/tasks/s3-from")).build();
        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(0));

        list = list().prefix(Property.ofValue("/tasks/s3-move")).build();
        listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(2));
    }

    @Test
    void moveWithDynamicMoveToKey() throws Exception {
        this.createBucket();

        upload("/tasks/s3-dynamic-from");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(Property.ofExpression("{{ bucket }}"))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .prefix(Property.ofValue("/tasks/s3-dynamic-from"))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveTo(
                Copy.CopyObject.builder()
                    .bucket(Property.ofExpression("{{ bucket }}"))
                    .key(Property.ofExpression("/tasks/s3-dynamic-to/{{ dest }}"))
                    .build()
            )
            .build();

        task.run(runContextFactory.of(Map.of("bucket", this.BUCKET, "dest", "2026/08/27")));

        // the source is gone: the copy ran and deleted it
        List list = list().prefix(Property.ofValue("/tasks/s3-dynamic-from")).build();
        assertThat(list.run(runContext(list)).getObjects().size(), is(0));

        // the destination key resolved the expression instead of keeping the literal template
        list = list().prefix(Property.ofValue("/tasks/s3-dynamic-to/2026/08/27")).build();
        assertThat(list.run(runContext(list)).getObjects().size(), is(1));
    }

    @Test
    void maxFilesExceeded() throws Exception {
        this.createBucket();

        // Upload 5 files
        for (int i = 0; i < 5; i++) {
            upload("/tasks/s3-maxfiles");
        }

        // Downloads with maxFiles=3 (less than 5 files) - should return first 3 files (truncated)
        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .prefix(Property.ofValue("/tasks/s3-maxfiles"))
            .maxFiles(Property.ofValue(3))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(3));
    }

    @Test
    void validateChecksum() throws Exception {
        this.createBucket();

        String basePrefix = "/tasks/s3-checksum-" + IdUtils.create();

        URI source1 = storagePut(null);
        URI source2 = storagePut(null);
        String sha1 = sha256Base64(source1);
        String sha2 = sha256Base64(source2);
        String key1 = basePrefix + "/" + IdUtils.create() + ".yml";
        String key2 = basePrefix + "/" + IdUtils.create() + ".yml";

        uploadWithSha256(source1, key1, sha1);
        uploadWithSha256(source2, key2, sha2);

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .prefix(Property.ofValue(basePrefix))
            .validateChecksum(Property.ofValue(true))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(2));
        for (S3Object object : run.getObjects()) {
            assertThat(object.getChecksumAlgorithm(), is("SHA256"));
            assertThat(object.getChecksumValue(), notNullValue());
        }
    }

    private void uploadWithSha256(URI source, String key, String sha256Base64) throws Exception {
        Upload upload = Upload.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(source.toString())
            .key(Property.ofValue(key))
            .checksumAlgorithm(Property.ofValue(ChecksumAlgorithm.SHA256))
            .checksum(Property.ofValue(sha256Base64))
            .build();
        upload.run(runContext(upload));
    }

    private String sha256Base64(URI storageUri) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream in = storageInterface.get(TenantService.MAIN_TENANT, null, storageUri)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) {
                digest.update(buf, 0, n);
            }
        }
        return Base64.getEncoder().encodeToString(digest.digest());
    }

    @Test
    void maxFilesNotExceeded() throws Exception {
        this.createBucket();

        // Upload 5 files
        for (int i = 0; i < 5; i++) {
            upload("/tasks/s3-maxfiles-ok");
        }

        // Downloads with maxFiles=10 (more than 5 files) - should return all 5 files
        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .prefix(Property.ofValue("/tasks/s3-maxfiles-ok"))
            .maxFiles(Property.ofValue(10))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(5));
    }

    private void uploadContent(String key, byte[] content) throws Exception {
        var source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new ByteArrayInputStream(content)
        );
        var upload = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(source.toString())
            .key(Property.ofValue(key))
            .build();
        upload.run(runContext(upload));
    }

    @Test
    void shouldPreserveSubdirectoriesAndSkipDirectoryMarkersOnMove() throws Exception {
        this.createBucket();

        var content2023 = "year,sales\n2023,100";
        var content2024 = "year,sales\n2024,200";

        uploadContent("incoming/2023/report.csv", content2023.getBytes(StandardCharsets.UTF_8));
        uploadContent("incoming/2024/report.csv", content2024.getBytes(StandardCharsets.UTF_8));
        uploadContent("incoming/subfolder/", new byte[0]);

        var task = Downloads.builder()
            .id(IdUtils.create())
            .type(Downloads.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .prefix(Property.ofValue("incoming/"))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveTo(
                Copy.CopyObject.builder()
                    .key(Property.ofValue("archive/"))
                    .build()
            )
            .build();

        var run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(2));
        assertThat(run.getOutputFiles().size(), is(2));

        var listIncoming = list().prefix(Property.ofValue("incoming/")).build();
        var incomingResult = listIncoming.run(runContext(listIncoming));
        assertThat(incomingResult.getObjects().size(), is(1));
        assertThat(incomingResult.getObjects().getFirst().getKey(), is("incoming/subfolder/"));

        var listArchive = list().prefix(Property.ofValue("archive/")).build();
        var archiveResult = listArchive.run(runContext(listArchive));
        assertThat(archiveResult.getObjects().size(), is(2));

        var movedKeys = archiveResult.getObjects().stream().map(S3Object::getKey).toList();
        assertThat(movedKeys, containsInAnyOrder("archive/2023/report.csv", "archive/2024/report.csv"));

        var download2023 = Download.builder()
            .id(IdUtils.create())
            .type(Download.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .key(Property.ofValue("archive/2023/report.csv"))
            .build();
        var out2023 = download2023.run(runContext(download2023));
        try (var is = storageInterface.get(TenantService.MAIN_TENANT, null, out2023.getUri())) {
            var readContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(readContent, is(content2023));
        }

        var download2024 = Download.builder()
            .id(IdUtils.create())
            .type(Download.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .key(Property.ofValue("archive/2024/report.csv"))
            .build();
        var out2024 = download2024.run(runContext(download2024));
        try (var is = storageInterface.get(TenantService.MAIN_TENANT, null, out2024.getUri())) {
            var readContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(readContent, is(content2024));
        }
    }

    @Test
    void shouldPreventDataLossOnMoveCollision() throws Exception {
        this.createBucket();

        uploadContent("folderA/file.txt", "data1".getBytes(StandardCharsets.UTF_8));
        uploadContent("folderB/file.txt", "data2".getBytes(StandardCharsets.UTF_8));

        var task = Downloads.builder()
            .id(IdUtils.create())
            .type(Downloads.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveTo(
                Copy.CopyObject.builder()
                    .key(Property.ofValue("archive/"))
                    .build()
            )
            .build();

        assertThrows(
            IllegalStateException.class,
            () -> task.run(runContext(task))
        );

        var listFolderA = list().prefix(Property.ofValue("folderA/")).build();
        assertThat(listFolderA.run(runContext(listFolderA)).getObjects().size(), is(1));

        var listFolderB = list().prefix(Property.ofValue("folderB/")).build();
        assertThat(listFolderB.run(runContext(listFolderB)).getObjects().size(), is(1));
    }
}

package io.kestra.plugin.aws.s3;

import java.io.InputStream;
import java.net.URI;
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
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
            .prefix(Property.ofValue("/tasks/s3-maxfiles-ok"))
            .maxFiles(Property.ofValue(10))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getObjects().size(), is(5));
    }
}

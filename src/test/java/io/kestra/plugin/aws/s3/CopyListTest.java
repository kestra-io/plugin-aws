package io.kestra.plugin.aws.s3;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.s3.CopyList.CopyListDestination;
import io.kestra.plugin.aws.s3.CopyList.CopyListSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CopyListTest extends AbstractTest {

    @Test
    void shouldCopyFilesBetweenBuckets() throws Exception {
        String sourceBucket = this.createBucket();
        String destBucket = this.createBucket(IdUtils.create().toLowerCase());

        String prefix = "tasks/s3/" + IdUtils.create() + "/source";
        for (int i = 0; i < 3; i++) {
            this.upload(prefix, sourceBucket);
        }

        CopyList task = CopyList.builder()
            .id(CopyListTest.class.getSimpleName())
            .type(CopyList.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(
                CopyListSource.builder()
                    .bucket(Property.ofValue(sourceBucket))
                    .prefix(Property.ofValue(prefix))
                    .build()
            )
            .to(
                CopyListDestination.builder()
                    .bucket(Property.ofValue(destBucket))
                    .prefix(Property.ofValue("tasks/s3/dest"))
                    .build()
            )
            .delete(Property.ofValue(false))
            .build();

        CopyList.Output run = task.run(runContext(task));
        assertThat(run.getCount()).isEqualTo(3L);
        assertThat(run.getSize()).isPositive();
        assertThat(run.getObjects()).hasSize(3);

        // Verify destination has the copied files
        List listDest = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .bucket(Property.ofValue(destBucket))
            .prefix(Property.ofValue("tasks/s3/dest"))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .build();
        List.Output destOutput = listDest.run(runContext(listDest));
        assertThat(destOutput.getObjects()).hasSize(3);

        // Verify source files still exist
        List listSource = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .bucket(Property.ofValue(sourceBucket))
            .prefix(Property.ofValue(prefix))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .build();
        List.Output sourceOutput = listSource.run(runContext(listSource));
        assertThat(sourceOutput.getObjects()).hasSize(3);
    }

    @Test
    void shouldMoveFilesWithinBucketWhenDeleteIsTrue() throws Exception {
        String bucket = this.createBucket();
        String sourcePrefix = "tasks/s3/" + IdUtils.create() + "/inbound";
        String destPrefix = "tasks/s3/" + IdUtils.create() + "/archive";

        for (int i = 0; i < 3; i++) {
            this.upload(sourcePrefix, bucket);
        }

        CopyList task = CopyList.builder()
            .id(CopyListTest.class.getSimpleName())
            .type(CopyList.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(
                CopyListSource.builder()
                    .bucket(Property.ofValue(bucket))
                    .prefix(Property.ofValue(sourcePrefix))
                    .build()
            )
            .to(
                CopyListDestination.builder()
                    .prefix(Property.ofValue(destPrefix))
                    .build()
            )
            .delete(Property.ofValue(true))
            .build();

        CopyList.Output run = task.run(runContext(task));
        assertThat(run.getCount()).isEqualTo(3L);

        // Destination has 3 files
        List listDest = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue(destPrefix))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .build();
        List.Output destOutput = listDest.run(runContext(listDest));
        assertThat(destOutput.getObjects()).hasSize(3);

        // Source has 0 files (deleted after move)
        List listSource = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue(sourcePrefix))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .build();
        List.Output sourceOutput = listSource.run(runContext(listSource));
        assertThat(sourceOutput.getObjects()).isEmpty();
    }

    @Test
    void shouldFilterFilesByRegex() throws Exception {
        String bucket = this.createBucket();
        String prefix = "tasks/s3/" + IdUtils.create();

        String file1 = this.upload(prefix, bucket);
        this.upload(prefix, bucket);

        // Only match file1
        CopyList task = CopyList.builder()
            .id(CopyListTest.class.getSimpleName())
            .type(CopyList.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(
                CopyListSource.builder()
                    .bucket(Property.ofValue(bucket))
                    .prefix(Property.ofValue(prefix))
                    .regexp(Property.ofValue(".*" + file1.substring(file1.lastIndexOf('/') + 1)))
                    .build()
            )
            .to(
                CopyListDestination.builder()
                    .prefix(Property.ofValue("tasks/s3/filtered"))
                    .build()
            )
            .delete(Property.ofValue(false))
            .build();

        CopyList.Output run = task.run(runContext(task));
        assertThat(run.getCount()).isEqualTo(1L);
        assertThat(run.getObjects()).hasSize(1);
    }

    @Test
    void shouldCopyFilesConcurrently() throws Exception {
        String bucket = this.createBucket();
        String sourcePrefix = "tasks/s3/" + IdUtils.create() + "/parallel";
        String destPrefix = "tasks/s3/" + IdUtils.create() + "/dest";

        for (int i = 0; i < 6; i++) {
            this.upload(sourcePrefix, bucket);
        }

        CopyList task = CopyList.builder()
            .id(CopyListTest.class.getSimpleName())
            .type(CopyList.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .concurrent(4)
            .from(
                CopyListSource.builder()
                    .bucket(Property.ofValue(bucket))
                    .prefix(Property.ofValue(sourcePrefix))
                    .build()
            )
            .to(
                CopyListDestination.builder()
                    .prefix(Property.ofValue(destPrefix))
                    .build()
            )
            .delete(Property.ofValue(false))
            .build();

        CopyList.Output run = task.run(runContext(task));
        assertThat(run.getCount()).isEqualTo(6L);
    }

    @Test
    void shouldThrowExceptionWhenErrorOnEmptyAndNoFiles() throws Exception {
        String bucket = this.createBucket();

        CopyList task = CopyList.builder()
            .id(CopyListTest.class.getSimpleName())
            .type(CopyList.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .errorOnEmpty(Property.ofValue(true))
            .from(
                CopyListSource.builder()
                    .bucket(Property.ofValue(bucket))
                    .prefix(Property.ofValue("non-existent-prefix/"))
                    .build()
            )
            .to(
                CopyListDestination.builder()
                    .prefix(Property.ofValue("dest/"))
                    .build()
            )
            .build();

        assertThatThrownBy(() -> task.run(runContext(task)))
            .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void shouldThrowExceptionWhenMovingToSameLocation() throws Exception {
        String bucket = this.createBucket();
        String prefix = "tasks/s3/" + IdUtils.create();
        this.upload(prefix, bucket);

        CopyList task = CopyList.builder()
            .id(CopyListTest.class.getSimpleName())
            .type(CopyList.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .delete(Property.ofValue(true))
            .from(
                CopyListSource.builder()
                    .bucket(Property.ofValue(bucket))
                    .prefix(Property.ofValue(prefix))
                    .build()
            )
            .to(
                CopyListDestination.builder()
                    .prefix(Property.ofValue(prefix))
                    .build()
            )
            .build();

        assertThatThrownBy(() -> task.run(runContext(task)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("source and destination bucket and key are identical");
    }

    @Test
    void shouldThrowExceptionWhenMultipleSourcesCollideOnDestinationKey() throws Exception {
        String bucket = this.createBucket();
        String testId = IdUtils.create();
        this.update("tasks/s3/" + testId + "/folderA/item.txt", bucket);
        this.update("tasks/s3/" + testId + "/folderB/item.txt", bucket);

        CopyList task = CopyList.builder()
            .id(CopyListTest.class.getSimpleName())
            .type(CopyList.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .from(
                CopyListSource.builder()
                    .bucket(Property.ofValue(bucket))
                    .regexp(Property.ofValue("tasks/s3/" + testId + "/.*/item\\.txt"))
                    .build()
            )
            .to(
                CopyListDestination.builder()
                    .prefix(Property.ofValue("tasks/s3/" + testId + "/dest"))
                    .build()
            )
            .build();

        assertThatThrownBy(() -> task.run(runContext(task)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("multiple source objects would overwrite the same destination key");
    }
}

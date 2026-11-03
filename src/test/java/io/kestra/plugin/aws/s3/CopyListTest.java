package io.kestra.plugin.aws.s3;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.s3.models.S3Object;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CopyListTest extends AbstractTest {

    @Test
    void shouldCopyBetweenBucketsKeepingThePathUnderThePrefix() throws Exception {
        var sourceBucket = this.createBucket();
        var destBucket = this.createBucket(IdUtils.create().toLowerCase());
        var prefix = "tasks/s3/%s/source".formatted(IdUtils.create());
        var first = this.upload(prefix, sourceBucket);
        var second = this.upload("%s/nested".formatted(prefix), sourceBucket);

        var run = copyList(sourceBucket)
            .prefix(Property.ofValue(prefix))
            .to(CopyList.Destination.builder().bucket(Property.ofValue(destBucket)).prefix(Property.ofValue("dest/")).build())
            .build();
        var output = run.run(runContext(run));

        assertThat(output.getCount()).isEqualTo(2L);
        assertThat(output.getSize()).isPositive();
        assertThat(keys(destBucket, "dest")).containsExactlyInAnyOrder(
            "dest/%s".formatted(first.substring(prefix.length() + 1)),
            "dest/%s".formatted(second.substring(prefix.length() + 1))
        );
        assertThat(keys(sourceBucket, prefix)).hasSize(2);
    }

    @Test
    void shouldKeepTheFullKeyWhenNoPrefixIsSet() throws Exception {
        var bucket = this.createBucket();
        var destBucket = this.createBucket(IdUtils.create().toLowerCase());
        var testId = IdUtils.create();
        this.update("tasks/s3/%s/folderA/item.txt".formatted(testId), bucket);
        this.update("tasks/s3/%s/folderB/item.txt".formatted(testId), bucket);

        var run = copyList(bucket)
            .regexp(Property.ofValue("tasks/s3/%s/.*/item\\.txt".formatted(testId)))
            .to(CopyList.Destination.builder().bucket(Property.ofValue(destBucket)).prefix(Property.ofValue("copies")).build())
            .build();
        var output = run.run(runContext(run));

        assertThat(output.getCount()).isEqualTo(2L);
        assertThat(keys(destBucket, "copies")).containsExactlyInAnyOrder(
            "copies/tasks/s3/%s/folderA/item.txt".formatted(testId),
            "copies/tasks/s3/%s/folderB/item.txt".formatted(testId)
        );
    }

    @Test
    void shouldRefuseADestinationInsideTheSourcePrefixOfTheSameBucket() throws Exception {
        var bucket = this.createBucket();
        var prefix = "tasks/s3/%s/inbound".formatted(IdUtils.create());
        var existing = this.update("%s/processed/a.yml".formatted(prefix), bucket);
        this.upload(prefix, bucket);

        var run = copyList(bucket)
            .prefix(Property.ofValue(prefix))
            .to(CopyList.Destination.builder().prefix(Property.ofValue("%s/processed".formatted(prefix))).build())
            .delete(Property.ofValue(true))
            .build();

        assertThatThrownBy(() -> run.run(runContext(run)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("inside the listed source range");
        assertThat(keys(bucket, prefix)).hasSize(2).contains(existing);
    }

    @Test
    void shouldMoveConcurrentlyAndEmptyTheSource() throws Exception {
        var bucket = this.createBucket();
        var sourcePrefix = "tasks/s3/%s/inbound".formatted(IdUtils.create());
        var destPrefix = "tasks/s3/%s/archive".formatted(IdUtils.create());
        for (var i = 0; i < 6; i++) {
            this.upload(sourcePrefix, bucket);
        }

        var run = copyList(bucket)
            .prefix(Property.ofValue(sourcePrefix))
            .to(CopyList.Destination.builder().prefix(Property.ofValue(destPrefix)).build())
            .delete(Property.ofValue(true))
            .concurrent(Property.ofValue(4))
            .build();
        var output = run.run(runContext(run));

        assertThat(output.getCount()).isEqualTo(6L);
        assertThat(keys(bucket, destPrefix)).hasSize(6);
        assertThat(keys(bucket, sourcePrefix)).isEmpty();
    }

    @Test
    void shouldCopyOnlyTheObjectsMatchingTheRegexp() throws Exception {
        var bucket = this.createBucket();
        var prefix = "tasks/s3/%s/filter".formatted(IdUtils.create());
        var kept = this.upload(prefix, bucket);
        this.upload(prefix, bucket);

        var run = copyList(bucket)
            .prefix(Property.ofValue(prefix))
            .regexp(Property.ofValue(".*%s".formatted(kept.substring(kept.lastIndexOf('/') + 1))))
            .to(CopyList.Destination.builder().prefix(Property.ofValue("tasks/s3/%s/filtered".formatted(IdUtils.create()))).build())
            .build();

        assertThat(run.run(runContext(run)).getCount()).isEqualTo(1L);
    }

    @Test
    void shouldStopAtMaxKeys() throws Exception {
        var bucket = this.createBucket();
        var sourcePrefix = "tasks/s3/%s/many".formatted(IdUtils.create());
        var destPrefix = "tasks/s3/%s/limited".formatted(IdUtils.create());
        for (var i = 0; i < 3; i++) {
            this.upload(sourcePrefix, bucket);
        }

        var run = copyList(bucket)
            .prefix(Property.ofValue(sourcePrefix))
            .maxKeys(Property.ofValue(2))
            .to(CopyList.Destination.builder().prefix(Property.ofValue(destPrefix)).build())
            .delete(Property.ofValue(true))
            .build();

        assertThat(run.run(runContext(run)).getCount()).isEqualTo(2L);
        assertThat(keys(bucket, destPrefix)).hasSize(2);
        assertThat(keys(bucket, sourcePrefix)).hasSize(1);
    }

    @Test
    void shouldFailWhenNothingMatchesAndErrorOnEmpty() throws Exception {
        var bucket = this.createBucket();

        var run = copyList(bucket)
            .prefix(Property.ofValue("tasks/s3/%s/missing".formatted(IdUtils.create())))
            .to(CopyList.Destination.builder().prefix(Property.ofValue("anywhere")).build())
            .errorOnEmpty(Property.ofValue(true))
            .build();

        assertThatThrownBy(() -> run.run(runContext(run)))
            .isInstanceOf(NoSuchElementException.class)
            .hasMessageContaining("Check `prefix` and `regexp`");
    }

    @Test
    void shouldRefuseToCopyAnObjectOntoItself() throws Exception {
        var bucket = this.createBucket();
        var prefix = "tasks/s3/%s/self/".formatted(IdUtils.create());
        this.upload(prefix.substring(0, prefix.length() - 1), bucket);

        var run = copyList(bucket)
            .prefix(Property.ofValue(prefix))
            .delimiter(Property.ofValue("/"))
            .to(CopyList.Destination.builder().prefix(Property.ofValue(prefix)).build())
            .delete(Property.ofValue(true))
            .build();

        assertThatThrownBy(() -> run.run(runContext(run)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot move")
            .hasMessageContaining("onto itself");
        assertThat(keys(bucket, prefix)).hasSize(1);
    }

    @Test
    void shouldMapKeysRelativeToTheSourcePrefix() {
        assertThat(CopyList.destinationKey("b/", "a/x/y.csv", "a/")).isEqualTo("b/x/y.csv");
        assertThat(CopyList.destinationKey("b", "a/x/y.csv", "a")).isEqualTo("b/x/y.csv");
        assertThat(CopyList.destinationKey("b", "a/x/y.csv", null)).isEqualTo("b/a/x/y.csv");
        assertThat(CopyList.destinationKey("", "a/x/y.csv", "a/")).isEqualTo("x/y.csv");
    }

    private CopyList.CopyListBuilder<?, ?> copyList(String bucket) {
        return CopyList.builder()
            .id(IdUtils.create())
            .type(CopyList.class.getName())
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .bucket(Property.ofValue(bucket));
    }

    private java.util.List<String> keys(String bucket, String prefix) throws Exception {
        var list = List.builder()
            .id(IdUtils.create())
            .type(List.class.getName())
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue(prefix))
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .build();
        return list.run(runContext(list)).getObjects().stream().map(S3Object::getKey).toList();
    }
}

package io.kestra.plugin.aws.s3;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.s3.models.S3Object;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class S3ServiceTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void shouldPreserveSubdirectoriesWhenMovingUnderPrefix() {
        var dest1 = S3Service.destinationKey("archive/", "incoming/2023/report.csv", "incoming/");
        var dest2 = S3Service.destinationKey("archive/", "incoming/2024/report.csv", "incoming/");

        assertThat(dest1).isEqualTo("archive/2023/report.csv");
        assertThat(dest2).isEqualTo("archive/2024/report.csv");
        assertThat(dest1).isNotEqualTo(dest2);
    }

    @Test
    void shouldPreserveSubdirectoriesWhenPrefixDoesNotEndOnFolderBoundary() {
        var dest1 = S3Service.destinationKey("archive/", "data/2023-01/report.csv", "data/2023");
        var dest2 = S3Service.destinationKey("archive/", "data/2023-02/report.csv", "data/2023");

        assertThat(dest1).isEqualTo("archive/2023-01/report.csv");
        assertThat(dest2).isEqualTo("archive/2023-02/report.csv");
        assertThat(dest1).isNotEqualTo(dest2);
    }

    @Test
    void shouldPreserveDeepSubdirectoriesWithPrefixWithoutTrailingSlash() {
        var dest = S3Service.destinationKey("archive", "incoming/nested/deep/report.csv", "incoming");
        assertThat(dest).isEqualTo("archive/nested/deep/report.csv");
    }

    @Test
    void shouldPreserveFilenameForFlatFileUnderPrefix() {
        var dest = S3Service.destinationKey("archive/", "incoming/report.csv", "incoming/");
        assertThat(dest).isEqualTo("archive/report.csv");
    }

    @Test
    void shouldHandleLeadingSlashesInPrefixAndKeys() {
        var dest = S3Service.destinationKey("/tasks/s3-move", "/tasks/s3-from/sub/file.yml", "/tasks/s3-from");
        assertThat(dest).isEqualTo("/tasks/s3-move/sub/file.yml");
    }

    @Test
    void shouldFallbackToFilenameWhenPrefixIsNull() {
        var dest = S3Service.destinationKey("/tasks/s3-move", "/tasks/s3-from/file.yml", null);
        assertThat(dest).isEqualTo("/tasks/s3-move/file.yml");
    }

    @Test
    void shouldFallbackToFilenameWhenPrefixDoesNotMatch() {
        var dest = S3Service.destinationKey("archive/", "other-folder/file.csv", "incoming");
        assertThat(dest).isEqualTo("archive/file.csv");
    }

    @Test
    void shouldHandleEmptyMoveToKey() {
        var dest = S3Service.destinationKey("", "incoming/2023/report.csv", "incoming/");
        assertThat(dest).isEqualTo("2023/report.csv");
    }

    @Test
    void shouldHandleEmptySubpath() {
        var dest = S3Service.destinationKey("archive/", "", "incoming/");
        assertThat(dest).isEqualTo("archive");
    }

    @Test
    void shouldIdentifyDirectoryMarkers() {
        assertThat(S3Service.isDirectoryMarker("folder/")).isTrue();
        assertThat(S3Service.isDirectoryMarker("folder/sub/")).isTrue();
        assertThat(S3Service.isDirectoryMarker(S3Object.builder().key("folder/").build())).isTrue();
        assertThat(S3Service.isDirectoryMarker("folder/file.csv")).isFalse();
        assertThat(S3Service.isDirectoryMarker(S3Object.builder().key("folder/file.csv").build())).isFalse();
        assertThat(S3Service.isDirectoryMarker((String) null)).isFalse();
        assertThat(S3Service.isDirectoryMarker((S3Object) null)).isFalse();
    }

    @Test
    void shouldThrowWhenMoveTargetsFindsCollision() {
        var runContext = runContextFactory.of();
        var objects = List.of(
            S3Object.builder().key("folderA/report.csv").build(),
            S3Object.builder().key("folderB/report.csv").build()
        );

        var downloads = Downloads.builder()
            .bucket(Property.ofValue("my-bucket"))
            .build();

        var moveTo = Copy.CopyObject.builder()
            .key(Property.ofValue("archive/"))
            .build();

        var ex = assertThrows(
            IllegalStateException.class,
            () -> S3Service.moveTargets(
                objects,
                Property.ofValue(ActionInterface.Action.MOVE),
                moveTo,
                runContext,
                downloads
            )
        );

        assertThat(ex.getMessage()).contains("multiple source objects would overwrite the same destination key");
        assertThat(ex.getMessage()).contains("Set a `prefix` that covers both folders");
    }

    @Test
    void shouldThrowWhenMultipleObjectsResolveToSameDestinationKey() {
        var runContext = runContextFactory.of();
        var objects = List.of(
            S3Object.builder().key("folderA/report.csv").build(),
            S3Object.builder().key("folderB/report.csv").build()
        );

        var downloads = Downloads.builder()
            .bucket(Property.ofValue("my-bucket"))
            .build();

        var moveTo = Copy.CopyObject.builder()
            .key(Property.ofValue("archive/"))
            .build();

        assertThrows(
            IllegalStateException.class,
            () -> S3Service.performAction(
                objects,
                Property.ofValue(ActionInterface.Action.MOVE),
                moveTo,
                runContext,
                downloads,
                downloads
            )
        );
    }
}

package io.kestra.plugin.aws.s3.files;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.s3.files.models.FileSystem;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@KestraTest
class S3FilesTaskTest {

    @Inject
    RunContextFactory runContextFactory;

    private RunContext runContext;

    @BeforeEach
    void setup() throws Exception {
        runContext = runContextFactory.of(Map.of());
    }

    // ──────────────────
    // CreateFileSystem
    // ──────────────────

    @Test
    void createFileSystem_parsesResponse() throws Exception {
        String responseJson = """
            {
              "fileSystemId": "fs-abc123",
              "fileSystemArn": "arn:aws:s3files:us-east-1:123456789012:file-system/fs-abc123",
              "status": "creating",
              "creationTime": 1712500000
            }
            """;

        CreateFileSystem task = CreateFileSystem.builder()
            .region(Property.ofValue("us-east-1"))
            .bucket(Property.ofValue("arn:aws:s3:::my-bucket"))
            .roleArn(Property.ofValue("arn:aws:iam::123456789012:role/S3FilesRole"))
            .build();

        CreateFileSystem spy = spy(task);
        doReturn(new S3FilesService.Response(201, responseJson))
            .when(spy).executeRequest(any(), any(), anyString(), any());

        CreateFileSystem.Output output = spy.run(runContext);

        assertThat(output.getFileSystemId()).isEqualTo("fs-abc123");
        assertThat(output.getFileSystemArn()).contains("fs-abc123");
        assertThat(output.getStatus()).isEqualTo("creating");
        assertThat(output.getCreationTime()).isEqualTo(1712500000L);
    }

    @Test
    void createFileSystem_includesOptionalFields() throws Exception {
        String responseJson = """
            {
              "fileSystemId": "fs-xyz",
              "fileSystemArn": "arn:aws:s3files:us-east-1:123456789012:file-system/fs-xyz",
              "status": "available",
              "creationTime": 1712600000
            }
            """;

        CreateFileSystem task = CreateFileSystem.builder()
            .region(Property.ofValue("us-east-1"))
            .bucket(Property.ofValue("arn:aws:s3:::my-bucket"))
            .roleArn(Property.ofValue("arn:aws:iam::123456789012:role/S3FilesRole"))
            .prefix(Property.ofValue("data/"))
            .kmsKeyId(Property.ofValue("alias/my-key"))
            .clientToken(Property.ofValue("unique-token-42"))
            .tags(Property.ofValue(Map.of("env", "prod", "team", "data")))
            .build();

        CreateFileSystem spy = spy(task);
        doReturn(new S3FilesService.Response(201, responseJson))
            .when(spy).executeRequest(any(), any(), anyString(), any());

        CreateFileSystem.Output output = spy.run(runContext);
        assertThat(output.getFileSystemId()).isEqualTo("fs-xyz");
    }

    // ────────────────
    // GetFileSystem
    // ────────────────

    @Test
    void getFileSystem_returnsFullModel() throws Exception {
        String responseJson = """
            {
              "fileSystemId": "fs-abc123",
              "fileSystemArn": "arn:aws:s3files:us-east-1:123456789012:file-system/fs-abc123",
              "status": "available",
              "bucket": "arn:aws:s3:::my-bucket",
              "roleArn": "arn:aws:iam::123456789012:role/S3FilesRole",
              "prefix": "data/",
              "creationTime": 1712500000,
              "ownerId": "123456789012"
            }
            """;

        GetFileSystem task = GetFileSystem.builder()
            .region(Property.ofValue("us-east-1"))
            .fileSystemId(Property.ofValue("fs-abc123"))
            .build();

        GetFileSystem spy = spy(task);
        doReturn(new S3FilesService.Response(200, responseJson))
            .when(spy).executeRequest(any(), any(), eq("/filesystems/fs-abc123"), any());

        GetFileSystem.Output output = spy.run(runContext);

        FileSystem fs = output.getFileSystem();
        assertThat(fs.getFileSystemId()).isEqualTo("fs-abc123");
        assertThat(fs.getStatus()).isEqualTo("available");
        assertThat(fs.getPrefix()).isEqualTo("data/");
        assertThat(fs.getOwnerId()).isEqualTo("123456789012");
    }

    // ────────────────
    // ListFileSystems
    // ────────────────

    @Test
    void listFileSystems_parsesListAndNextToken() throws Exception {
        String responseJson = """
            {
              "fileSystems": [
                {"fileSystemId": "fs-001", "status": "available"},
                {"fileSystemId": "fs-002", "status": "creating"}
              ],
              "nextToken": "page2token"
            }
            """;

        ListFileSystems task = ListFileSystems.builder()
            .region(Property.ofValue("us-east-1"))
            .maxResults(Property.ofValue(10))
            .build();

        ListFileSystems spy = spy(task);
        doReturn(new S3FilesService.Response(200, responseJson))
            .when(spy).executeRequest(any(), any(), anyString(), any());

        ListFileSystems.Output output = spy.run(runContext);

        assertThat(output.getFileSystems()).hasSize(2);
        assertThat(output.getFileSystems().get(0).getFileSystemId()).isEqualTo("fs-001");
        assertThat(output.getFileSystems().get(1).getStatus()).isEqualTo("creating");
        assertThat(output.getNextToken()).isEqualTo("page2token");
    }

    @Test
    void listFileSystems_emptyList() throws Exception {
        String responseJson = """
            {
              "fileSystems": []
            }
            """;

        ListFileSystems task = ListFileSystems.builder()
            .region(Property.ofValue("us-east-1"))
            .build();

        ListFileSystems spy = spy(task);
        doReturn(new S3FilesService.Response(200, responseJson))
            .when(spy).executeRequest(any(), any(), anyString(), any());

        ListFileSystems.Output output = spy.run(runContext);

        assertThat(output.getFileSystems()).isEmpty();
        assertThat(output.getNextToken()).isNull();
    }

    // ──────────────────
    // DeleteFileSystem
    // ──────────────────

    @Test
    void deleteFileSystem_callsCorrectPath() throws Exception {
        DeleteFileSystem task = DeleteFileSystem.builder()
            .region(Property.ofValue("us-east-1"))
            .fileSystemId(Property.ofValue("fs-abc123"))
            .build();

        DeleteFileSystem spy = spy(task);
        doReturn(new S3FilesService.Response(204, ""))
            .when(spy).executeRequest(any(), any(), eq("/filesystems/fs-abc123"), any());

        DeleteFileSystem.Output output = spy.run(runContext);

        assertThat(output).isNotNull();
        verify(spy).executeRequest(any(), any(), eq("/filesystems/fs-abc123"), any());
    }

    // ───────────────────
    // CreateMountTarget
    // ───────────────────

    @Test
    void createMountTarget_parsesResponse() throws Exception {
        String responseJson = """
            {
              "mountTargetId": "mt-abc123",
              "fileSystemId": "fs-abc123",
              "subnetId": "subnet-abc",
              "ipAddress": "10.0.1.42",
              "status": "creating"
            }
            """;

        CreateMountTarget task = CreateMountTarget.builder()
            .region(Property.ofValue("us-east-1"))
            .fileSystemId(Property.ofValue("fs-abc123"))
            .subnetId(Property.ofValue("subnet-abc"))
            .build();

        CreateMountTarget spy = spy(task);
        doReturn(new S3FilesService.Response(201, responseJson))
            .when(spy).executeRequest(any(), any(), eq("/filesystems/fs-abc123/mounttargets"), any());

        CreateMountTarget.Output output = spy.run(runContext);

        assertThat(output.getMountTargetId()).isEqualTo("mt-abc123");
        assertThat(output.getIpAddress()).isEqualTo("10.0.1.42");
        assertThat(output.getStatus()).isEqualTo("creating");
    }

    @Test
    void createMountTarget_withSecurityGroups() throws Exception {
        String responseJson = """
            {
              "mountTargetId": "mt-xyz",
              "ipAddress": "10.0.2.11",
              "status": "creating"
            }
            """;

        CreateMountTarget task = CreateMountTarget.builder()
            .region(Property.ofValue("us-east-1"))
            .fileSystemId(Property.ofValue("fs-abc123"))
            .subnetId(Property.ofValue("subnet-abc"))
            .ipAddress(Property.ofValue("10.0.2.11"))
            .securityGroups(Property.ofValue(List.of("sg-001", "sg-002")))
            .build();

        CreateMountTarget spy = spy(task);
        doReturn(new S3FilesService.Response(201, responseJson))
            .when(spy).executeRequest(any(), any(), anyString(), any());

        CreateMountTarget.Output output = spy.run(runContext);
        assertThat(output.getMountTargetId()).isEqualTo("mt-xyz");
    }

    // ──────────────────
    // ListMountTargets
    // ──────────────────

    @Test
    void listMountTargets_parsesListAndNextToken() throws Exception {
        String responseJson = """
            {
              "mountTargets": [
                {"mountTargetId": "mt-001", "status": "available", "ipAddress": "10.0.1.5"},
                {"mountTargetId": "mt-002", "status": "creating", "ipAddress": "10.0.1.6"}
              ],
              "nextToken": "mt-page2"
            }
            """;

        ListMountTargets task = ListMountTargets.builder()
            .region(Property.ofValue("us-east-1"))
            .fileSystemId(Property.ofValue("fs-abc123"))
            .build();

        ListMountTargets spy = spy(task);
        doReturn(new S3FilesService.Response(200, responseJson))
            .when(spy).executeRequest(any(), any(), anyString(), any());

        ListMountTargets.Output output = spy.run(runContext);

        assertThat(output.getMountTargets()).hasSize(2);
        assertThat(output.getMountTargets().get(0).getMountTargetId()).isEqualTo("mt-001");
        assertThat(output.getMountTargets().get(0).getIpAddress()).isEqualTo("10.0.1.5");
        assertThat(output.getNextToken()).isEqualTo("mt-page2");
    }

    // ───────────────────
    // DeleteMountTarget
    // ───────────────────

    @Test
    void deleteMountTarget_callsCorrectPath() throws Exception {
        DeleteMountTarget task = DeleteMountTarget.builder()
            .region(Property.ofValue("us-east-1"))
            .mountTargetId(Property.ofValue("mt-abc123"))
            .build();

        DeleteMountTarget spy = spy(task);
        doReturn(new S3FilesService.Response(204, ""))
            .when(spy).executeRequest(any(), any(), eq("/mounttargets/mt-abc123"), any());

        DeleteMountTarget.Output output = spy.run(runContext);

        assertThat(output).isNotNull();
        verify(spy).executeRequest(any(), any(), eq("/mounttargets/mt-abc123"), any());
    }

    // ────────────────
    // Error handling
    // ────────────────

    @Test
    void task_throwsOnApiError() throws Exception {
        GetFileSystem task = GetFileSystem.builder()
            .region(Property.ofValue("us-east-1"))
            .fileSystemId(Property.ofValue("fs-nonexistent"))
            .build();

        GetFileSystem spy = spy(task);
        doThrow(new RuntimeException("S3 Files API error [HTTP 404]: {\"message\":\"FileSystem not found\"}"))
            .when(spy).executeRequest(any(), any(), anyString(), any());

        assertThatThrownBy(() -> spy.run(runContext))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("404");
    }
}

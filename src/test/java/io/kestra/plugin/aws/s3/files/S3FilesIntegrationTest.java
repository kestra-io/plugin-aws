package io.kestra.plugin.aws.s3.files;

import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractLocalStackTest;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled("LocalStack 3.4.0 does not yet support the S3 Files control-plane API")
class S3FilesIntegrationTest extends AbstractLocalStackTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void fullLifecycle() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        String endpointOverride = "http://localhost:4566";
        String region = "us-east-1";
        String bucketArn = "arn:aws:s3:::my-test-bucket";
        String roleArn = "arn:aws:iam::000000000000:role/S3FilesRole";
        String subnetId = "subnet-00000000";

        // 1. Create file system
        CreateFileSystem createFs = CreateFileSystem.builder()
            .endpointOverride(Property.ofValue(endpointOverride))
            .region(Property.ofValue(region))
            .bucket(Property.ofValue(bucketArn))
            .roleArn(Property.ofValue(roleArn))
            .build();

        CreateFileSystem.Output createFsOutput = createFs.run(runContext);
        assertThat(createFsOutput.getFileSystemId()).isNotNull();

        String fsId = createFsOutput.getFileSystemId();

        // 2. Get file system
        GetFileSystem getFs = GetFileSystem.builder()
            .endpointOverride(Property.ofValue(endpointOverride))
            .region(Property.ofValue(region))
            .fileSystemId(Property.ofValue(fsId))
            .build();

        GetFileSystem.Output getFsOutput = getFs.run(runContext);
        assertThat(getFsOutput.getFileSystem().getFileSystemId()).isEqualTo(fsId);

        // 3. List file systems
        ListFileSystems listFs = ListFileSystems.builder()
            .endpointOverride(Property.ofValue(endpointOverride))
            .region(Property.ofValue(region))
            .build();

        ListFileSystems.Output listFsOutput = listFs.run(runContext);
        assertThat(listFsOutput.getFileSystems()).anyMatch(f -> fsId.equals(f.getFileSystemId()));

        // 4. Create mount target
        CreateMountTarget createMt = CreateMountTarget.builder()
            .endpointOverride(Property.ofValue(endpointOverride))
            .region(Property.ofValue(region))
            .fileSystemId(Property.ofValue(fsId))
            .subnetId(Property.ofValue(subnetId))
            .build();

        CreateMountTarget.Output createMtOutput = createMt.run(runContext);
        assertThat(createMtOutput.getMountTargetId()).isNotNull();

        String mtId = createMtOutput.getMountTargetId();

        // 5. List mount targets
        ListMountTargets listMt = ListMountTargets.builder()
            .endpointOverride(Property.ofValue(endpointOverride))
            .region(Property.ofValue(region))
            .fileSystemId(Property.ofValue(fsId))
            .build();

        ListMountTargets.Output listMtOutput = listMt.run(runContext);
        assertThat(listMtOutput.getMountTargets()).anyMatch(m -> mtId.equals(m.getMountTargetId()));

        // 6. Delete mount target
        DeleteMountTarget deleteMt = DeleteMountTarget.builder()
            .endpointOverride(Property.ofValue(endpointOverride))
            .region(Property.ofValue(region))
            .mountTargetId(Property.ofValue(mtId))
            .build();
        deleteMt.run(runContext);

        // 7. Delete file system
        DeleteFileSystem deleteFs = DeleteFileSystem.builder()
            .endpointOverride(Property.ofValue(endpointOverride))
            .region(Property.ofValue(region))
            .fileSystemId(Property.ofValue(fsId))
            .build();
        deleteFs.run(runContext);
    }
}

package io.kestra.plugin.aws.s3.files;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.files.models.MountTarget;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.http.SdkHttpMethod;

/**
 * Creates an NFS mount target for an Amazon S3 Files file system in the
 * specified subnet.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(full = true, code = """
            id: aws_s3_files_create_mount_target
            namespace: company.team

            tasks:
              - id: create_mt
                type: io.kestra.plugin.aws.s3.files.CreateMountTarget
                accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                region: "us-east-1"
                fileSystemId: "fs-0123456789abcdef0"
                subnetId: "subnet-0123456789abcdef0"
            """)
    }
)
@Schema(title = "Create an Amazon S3 Files mount target", description = "Creates an NFS v4.1 mount target in a VPC subnet, enabling EC2 / Lambda / ECS workloads to mount the file system.")
public class CreateMountTarget extends AbstractS3Files implements RunnableTask<CreateMountTarget.Output> {

    @Schema(title = "File system ID", description = "The ID of the S3 Files file system for which to create the mount target.")
    @NotNull
    private Property<String> fileSystemId;

    @Schema(title = "Subnet ID", description = "The ID of the VPC subnet where the mount target will be created.")
    @NotNull
    private Property<String> subnetId;

    @Schema(title = "IP address", description = "Optional static IPv4 address for the mount target. When omitted, AWS assigns one automatically.")
    private Property<String> ipAddress;

    @Schema(title = "Security group IDs", description = "Up to 5 security group IDs to associate with the mount target's network interface.")
    @PluginProperty(dynamic = false)
    private Property<List<String>> securityGroups;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String fsId = runContext.render(fileSystemId).as(String.class).orElseThrow();

        Map<String, Object> body = new HashMap<>();
        body.put("subnetId", runContext.render(subnetId).as(String.class).orElseThrow());

        runContext.render(ipAddress).as(String.class).ifPresent(v -> body.put("ipAddress", v));

        if (securityGroups != null) {
            List<String> sgList = runContext.render(securityGroups).asList(String.class);
            if (!sgList.isEmpty()) {
                body.put("securityGroups", sgList);
            }
        }

        S3FilesService.Response response = executeRequest(
            runContext,
            SdkHttpMethod.POST,
            "/filesystems/" + fsId + "/mounttargets",
            S3FilesService.toJson(body)
        );

        MountTarget mt = S3FilesService.fromJson(response.body(), MountTarget.class);

        runContext.logger().info("Created S3 Files mount target: {} (status={})", mt.getMountTargetId(), mt.getStatus());

        return Output.builder()
            .mountTargetId(mt.getMountTargetId())
            .ipAddress(mt.getIpAddress())
            .status(mt.getStatus())
            .build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    @Schema(title = "Output of the CreateMountTarget task")
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Mount target ID")
        private String mountTargetId;

        @Schema(title = "IPv4 address of the NFS endpoint")
        private String ipAddress;

        @Schema(title = "Lifecycle status")
        private String status;
    }
}

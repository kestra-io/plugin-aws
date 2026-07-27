package io.kestra.plugin.aws.s3.files.models;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents an Amazon S3 Files mount target as returned by the control-plane API.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class MountTarget {

    @Schema(title = "Mount target ID", description = "Unique identifier of the mount target.")
    private String mountTargetId;

    @Schema(title = "File system ID", description = "ID of the file system this mount target belongs to.")
    private String fileSystemId;

    @Schema(title = "Subnet ID", description = "VPC subnet in which the mount target is created.")
    private String subnetId;

    @Schema(title = "IP address", description = "IP address assigned to the mount target.")
    private String ipAddress;

    @Schema(title = "Status", description = "Current lifecycle status of the mount target.")
    private String status;

    @Schema(title = "Status message", description = "Human-readable detail about the current status.")
    private String statusMessage;

    @Schema(title = "VPC ID", description = "VPC in which the mount target resides.")
    private String vpcId;

    @Schema(title = "Owner ID", description = "AWS account ID that owns the mount target.")
    private String ownerId;

    @Schema(title = "Security groups", description = "Security groups associated with the mount target.")
    private List<String> securityGroups;
}

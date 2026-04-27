package io.kestra.plugin.aws.s3.files.models;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

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

    private String mountTargetId;
    private String fileSystemId;
    private String subnetId;
    private String ipAddress;
    private String status;
    private String statusMessage;
    private String vpcId;
    private String ownerId;
    private List<String> securityGroups;
}

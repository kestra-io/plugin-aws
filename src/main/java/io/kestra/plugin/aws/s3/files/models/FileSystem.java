package io.kestra.plugin.aws.s3.files.models;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents an Amazon S3 Files file system resource as returned by the control-plane API.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileSystem {

    @Schema(title = "File system ID", description = "Unique identifier of the S3 Files file system.")
    private String fileSystemId;

    @Schema(title = "File system ARN", description = "The ARN of the file system.")
    private String fileSystemArn;

    @Schema(title = "Status", description = "Current lifecycle status of the file system.")
    private String status;

    @Schema(title = "Status message", description = "Human-readable detail about the current status.")
    private String statusMessage;

    @Schema(title = "Bucket", description = "The S3 bucket backing the file system.")
    private String bucket;

    @Schema(title = "Prefix", description = "The bucket prefix exposed by the file system, if any.")
    private String prefix;

    @Schema(title = "Role ARN", description = "IAM role ARN used by the file system to access the bucket.")
    private String roleArn;

    @Schema(title = "KMS key ID", description = "KMS key used for encryption, if configured.")
    private String kmsKeyId;

    @Schema(title = "Name", description = "The file system name.")
    private String name;

    @Schema(title = "Owner ID", description = "AWS account ID that owns the file system.")
    private String ownerId;

    @Schema(title = "Creation time", description = "Creation timestamp (epoch seconds).")
    private Long creationTime;

    @Schema(title = "Tags", description = "Resource tags attached to the file system.")
    private List<Tag> tags;

    @Schema(title = "Client token", description = "Idempotency token associated with the file system.")
    private String clientToken;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Tag {
        @Schema(title = "Key", description = "Tag key.")
        private String key;

        @Schema(title = "Value", description = "Tag value.")
        private String value;
    }
}

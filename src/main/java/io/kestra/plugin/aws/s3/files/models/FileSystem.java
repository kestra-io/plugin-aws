package io.kestra.plugin.aws.s3.files.models;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

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

    private String fileSystemId;
    private String fileSystemArn;
    private String status;
    private String statusMessage;
    private String bucket;
    private String prefix;
    private String roleArn;
    private String kmsKeyId;
    private String name;
    private String ownerId;
    private Long creationTime;
    private List<Tag> tags;
    private String clientToken;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Tag {
        private String key;
        private String value;
    }
}

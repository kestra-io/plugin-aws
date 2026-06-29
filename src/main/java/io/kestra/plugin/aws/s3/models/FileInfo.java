package io.kestra.plugin.aws.s3.models;

import java.net.URI;
import java.util.Map;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import io.kestra.core.models.annotations.PluginProperty;

@Builder
@Getter
public class FileInfo {
    @Schema(
        title = "The URI of the downloaded file in Kestra's storage"
    )
    @PluginProperty(group = "advanced")
    private URI uri;

    @Schema(
        title = "The size of the file in bytes"
    )
    @PluginProperty(group = "advanced")
    private Long contentLength;

    @Schema(
        title = "The MIME type of the file"
    )
    @PluginProperty(group = "advanced")
    private String contentType;

    @Schema(
        title = "The metadata of the file"
    )
    @PluginProperty(group = "advanced")
    private Map<String, String> metadata;

    @Schema(
        title = "The version ID of the file"
    )
    @PluginProperty(group = "advanced")
    private String versionId;

    @Schema(
        title = "An ETag is an opaque identifier assigned by a web server to a specific version of a resource found at a URL"
    )
    @PluginProperty(group = "advanced")
    private String eTag;

    @Schema(
        title = "Checksum algorithm reported by S3",
        description = "One of SHA1, SHA256, CRC32, CRC32C. Null when validateChecksum was not enabled or the object has no stored checksum."
    )
    @PluginProperty(group = "advanced")
    private String checksumAlgorithm;

    @Schema(
        title = "Checksum value reported by S3 (base64-encoded)",
        description = "Populated when validateChecksum is true and the object has a stored checksum."
    )
    @PluginProperty(group = "advanced")
    private String checksumValue;
}

package io.kestra.plugin.aws.s3.models;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.net.URI;
import java.util.Map;

@Builder
@Getter
public class FileInfo {
    @Schema(
        title = "The URI of the downloaded file in Kestra's storage"
    )
    private URI uri;

    @Schema(
        title = "The size of the file in bytes"
    )
    private Long contentLength;

    @Schema(
        title = "The MIME type of the file"
    )
    private String contentType;

    @Schema(
        title = "The metadata of the file"
    )
    private Map<String, String> metadata;

    @Schema(
        title = "The version ID of the file"
    )
    private String versionId;
}

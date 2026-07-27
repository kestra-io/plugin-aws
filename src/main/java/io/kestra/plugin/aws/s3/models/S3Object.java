package io.kestra.plugin.aws.s3.models;

import java.net.URI;
import java.time.Instant;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;
import lombok.With;

@Data
@Builder
public class S3Object {
    @With
    @Schema(title = "URI", description = "Internal storage URI of the downloaded object, when applicable.")
    URI uri;

    @Schema(title = "Key", description = "The object key within the bucket.")
    String key;

    @Schema(title = "ETag", description = "The entity tag (ETag) of the object.")
    String etag;

    @Schema(title = "Size", description = "The object size in bytes.")
    Long size;

    @Schema(title = "Last modified", description = "Timestamp of the object's last modification.")
    Instant lastModified;

    @Schema(title = "Owner", description = "The object owner.")
    Owner owner;

    @With
    @Schema(title = "Checksum algorithm", description = "The checksum algorithm used for the object, if any.")
    String checksumAlgorithm;

    @With
    @Schema(title = "Checksum value", description = "The object's checksum value, if any.")
    String checksumValue;

    public static S3Object of(software.amazon.awssdk.services.s3.model.S3Object object) {
        return S3Object.builder()
            .key(object.key())
            .etag(object.eTag())
            .size(object.size())
            .lastModified(object.lastModified())
            .owner(Owner.of(object.owner()))
            .build();
    }
}

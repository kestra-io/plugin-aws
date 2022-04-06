package io.kestra.plugin.aws.s3.models;

import lombok.Builder;
import lombok.Data;
import lombok.With;

import java.net.URI;
import java.time.Instant;

@Data
@Builder
public class S3Object {
    @With
    URI uri;
    String key;
    String etag;
    Long size;
    Instant lastModified;
    Owner owner;
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

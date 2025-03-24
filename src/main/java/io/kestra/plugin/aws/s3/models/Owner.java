package io.kestra.plugin.aws.s3.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Owner {
    String id;
    String displayName;

    public static Owner of(software.amazon.awssdk.services.s3.model.Owner object) {
        // this can happen in compatible S3 services
        if (object == null) {
            return null;
        }

        return Owner.builder()
            .id(object.id())
            .displayName(object.displayName())
            .build();
    }
}

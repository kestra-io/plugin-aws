package io.kestra.plugin.aws.s3.models;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Owner {
    @Schema(title = "Owner ID", description = "The S3 owner's canonical user ID.")
    String id;

    @Schema(title = "Display name", description = "The S3 owner's display name.")
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

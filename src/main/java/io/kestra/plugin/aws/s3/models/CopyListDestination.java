package io.kestra.plugin.aws.s3.models;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class CopyListDestination {

    @Schema(
        title = "The destination bucket name",
        description = "If omitted, defaults to the source bucket name."
    )
    @PluginProperty(group = "destination")
    private Property<String> bucket;

    @Schema(
        title = "The destination prefix (folder path)"
    )
    @PluginProperty(group = "destination")
    private Property<String> prefix;

    @Schema(
        title = "Server side encryption to apply to the target object",
        description = "Example: AES256 or AWS_KMS"
    )
    @PluginProperty(group = "destination")
    private Property<S3ServerSideEncryption> serverSideEncryption;

    @Schema(
        title = "KMS Key ARN or Key ID to use when server side encryption is AWS_KMS"
    )
    @PluginProperty(group = "destination")
    private Property<String> kmsKeyId;
}

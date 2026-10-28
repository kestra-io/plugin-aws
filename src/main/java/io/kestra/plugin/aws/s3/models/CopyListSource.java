package io.kestra.plugin.aws.s3.models;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.aws.s3.AbstractS3Object;
import io.kestra.plugin.aws.s3.ListInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
public class CopyListSource extends AbstractS3Object implements ListInterface {

    @Schema(
        title = "The S3 bucket to list and copy objects from"
    )
    @NotNull
    @PluginProperty(group = "source")
    private Property<String> bucket;

    @Schema(
        title = "Limits the response to keys that begin with the specified prefix"
    )
    @PluginProperty(group = "source")
    private Property<String> prefix;

    @Schema(
        title = "A delimiter is a character you use to group keys"
    )
    @PluginProperty(group = "processing")
    private Property<String> delimiter;

    @Schema(
        title = "Marker is where you want Amazon S3 to start listing from",
        description = "Amazon S3 starts listing after this specified key."
    )
    @PluginProperty(group = "advanced")
    private Property<String> marker;

    @Schema(
        title = "The EncodingType property for this object"
    )
    @PluginProperty(group = "advanced")
    private Property<String> encodingType;

    @Builder.Default
    @Schema(
        title = "Maximum number of objects returned across all paginated S3 calls",
        description = "Total upper bound on the objects returned. Default 1000."
    )
    @PluginProperty(group = "execution")
    private Property<Integer> maxKeys = Property.ofValue(1000);

    @Schema(
        title = "The account ID of the expected bucket owner"
    )
    @PluginProperty(group = "advanced")
    private Property<String> expectedBucketOwner;

    @Schema(
        title = "A regexp to filter on full key"
    )
    @PluginProperty(group = "processing")
    private Property<String> regexp;

    @Builder.Default
    @Schema(
        title = "The type of objects to filter: files, directory, or both"
    )
    @PluginProperty(group = "processing")
    private Property<Filter> filter = Property.ofValue(Filter.BOTH);
}

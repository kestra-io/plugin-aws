package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface ListInterface {
    @Schema(
        title = "The S3 bucket where to download the file."
    )
    @NotNull
    Property<String> getBucket();

    @Schema(
        title = "Limits the response to keys that begin with the specified prefix."
    )
    Property<String> getPrefix();

    @Schema(
        title = "A delimiter is a character you use to group keys."
    )
    Property<String> getDelimiter();

    @Schema(
        title = "Marker is where you want Amazon S3 to start listing from.",
        description = "Amazon S3 starts listing after this specified key. Marker can be any key in the bucket."
    )
    Property<String> getMarker();

    @Schema(
        title = "The EncodingType property for this object."
    )
    Property<String> getEncodingType();

    @Schema(
        title = "Sets the maximum number of keys returned in the response.",
        description = "By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more."
    )
    Property<Integer> getMaxKeys();

    @Schema(
        title = "The account ID of the expected bucket owner.",
        description = "If the bucket is owned by a different account, the request fails with the HTTP status code 403 Forbidden (access denied)."
    )
    Property<String> getExpectedBucketOwner();

    @Schema(
        title = "A regexp to filter on full key.",
        description = "ex:\n"+
            "`regExp: .*` to match all files\n"+
            "`regExp: .*2020-01-0.\\\\.csv` to match files between 01 and 09 of january ending with `.csv`"
    )
    Property<String> getRegexp();

    @Schema(
        title = "The type of objects to filter: files, directory, or both."
    )
    Property<Filter> getFilter();


    enum Filter {
        FILES,
        DIRECTORY,
        BOTH
    }
}

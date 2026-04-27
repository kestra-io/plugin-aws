package io.kestra.plugin.aws.s3.files;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.files.models.MountTarget;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.http.SdkHttpMethod;

/**
 * Lists all mount targets for an Amazon S3 Files file system.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: aws_s3_files_list_mount_targets
                namespace: company.team

                tasks:
                  - id: list_mt
                    type: io.kestra.plugin.aws.s3.files.ListMountTargets
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "us-east-1"
                    fileSystemId: "fs-0123456789abcdef0"
                """
        )
    }
)
@Schema(
    title = "List Amazon S3 Files mount targets",
    description = "Returns a paginated list of mount targets for the specified S3 Files file system."
)
public class ListMountTargets extends AbstractS3Files implements RunnableTask<ListMountTargets.Output> {

    @Schema(title = "File system ID", description = "The ID of the file system whose mount targets to list.")
    @NotNull
    private Property<String> fileSystemId;

    @Schema(title = "Maximum results", description = "Maximum number of mount targets to return per page.")
    private Property<Integer> maxResults;

    @Schema(title = "Pagination token", description = "Token from a previous call to retrieve the next page.")
    private Property<String> nextToken;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String fsId = runContext.render(fileSystemId).as(String.class).orElseThrow();

        StringBuilder path = new StringBuilder("/filesystems/" + fsId + "/mounttargets");
        String sep = "?";

        Integer maxVal = maxResults != null
            ? runContext.render(maxResults).as(Integer.class).orElse(null)
            : null;
        if (maxVal != null) {
            path.append(sep).append("maxResults=").append(maxVal);
            sep = "&";
        }

        String tokenVal = nextToken != null
            ? runContext.render(nextToken).as(String.class).orElse(null)
            : null;
        if (tokenVal != null) {
            path.append(sep).append("nextToken=").append(
                java.net.URLEncoder.encode(tokenVal, java.nio.charset.StandardCharsets.UTF_8)
            );
        }

        S3FilesService.Response response = executeRequest(
            runContext,
            SdkHttpMethod.GET,
            path.toString(),
            null
        );

        ListMountTargetsResponse parsed = S3FilesService.fromJson(response.body(), ListMountTargetsResponse.class);

        runContext.logger().info(
            "Listed {} mount target(s) for file system: {}",
            parsed.getMountTargets() != null ? parsed.getMountTargets().size() : 0, fsId
        );

        return Output.builder()
            .mountTargets(parsed.getMountTargets() != null ? parsed.getMountTargets() : List.of())
            .nextToken(parsed.getNextToken())
            .build();
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ListMountTargetsResponse {
        private List<MountTarget> mountTargets;
        private String nextToken;
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    @Schema(title = "Output of the ListMountTargets task")
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "List of mount targets")
        private List<MountTarget> mountTargets;

        @Schema(title = "Pagination token for the next page")
        private String nextToken;
    }
}

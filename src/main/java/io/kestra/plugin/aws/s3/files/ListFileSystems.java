package io.kestra.plugin.aws.s3.files;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.files.models.FileSystem;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.http.SdkHttpMethod;

/**
 * Lists Amazon S3 Files file systems, with optional filtering by bucket ARN and pagination.
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
                id: aws_s3_files_list_filesystems
                namespace: company.team

                tasks:
                  - id: list_fs
                    type: io.kestra.plugin.aws.s3.files.ListFileSystems
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "us-east-1"
                    bucket: "arn:aws:s3:::my-bucket"
                    maxResults: 10
                """
        )
    }
)
@Schema(
    title = "List Amazon S3 Files file systems",
    description = "Returns a paginated list of S3 Files file systems, optionally filtered by bucket ARN."
)
public class ListFileSystems extends AbstractS3Files implements RunnableTask<ListFileSystems.Output> {

    @Schema(title = "Bucket ARN filter", description = "When set, returns only file systems backed by this bucket ARN.")
    private Property<String> bucket;

    @Schema(title = "Maximum results", description = "Maximum number of file systems to return per page.")
    private Property<Integer> maxResults;

    @Schema(title = "Pagination token", description = "Token returned from a previous call to retrieve the next page.")
    private Property<String> nextToken;

    @Override
    public Output run(RunContext runContext) throws Exception {
        StringBuilder path = new StringBuilder("/filesystems");
        String sep = "?";

        String bucketVal = bucket != null
            ? runContext.render(bucket).as(String.class).orElse(null)
            : null;
        if (bucketVal != null) {
            path.append(sep).append("bucket=").append(java.net.URLEncoder.encode(bucketVal, java.nio.charset.StandardCharsets.UTF_8));
            sep = "&";
        }

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
            path.append(sep).append("nextToken=").append(java.net.URLEncoder.encode(tokenVal, java.nio.charset.StandardCharsets.UTF_8));
        }

        S3FilesService.Response response = executeRequest(
            runContext,
            SdkHttpMethod.GET,
            path.toString(),
            null
        );

        ListFileSystemsResponse parsed = S3FilesService.fromJson(response.body(), ListFileSystemsResponse.class);

        runContext.logger().info("Listed {} S3 Files file system(s)", parsed.getFileSystems() != null ? parsed.getFileSystems().size() : 0);

        return Output.builder()
            .fileSystems(parsed.getFileSystems())
            .nextToken(parsed.getNextToken())
            .build();
    }

    /** Internal wrapper matching the API list response envelope. */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ListFileSystemsResponse {
        private List<FileSystem> fileSystems;
        private String nextToken;
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    @Schema(title = "Output of the ListFileSystems task")
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "List of file systems")
        private List<FileSystem> fileSystems;

        @Schema(title = "Pagination token for the next page")
        private String nextToken;
    }
}

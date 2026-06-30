package io.kestra.plugin.aws.s3.files;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.files.models.FileSystem;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.http.SdkHttpMethod;

/**
 * Retrieves the full details of a single Amazon S3 Files file system by ID.
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
                id: aws_s3_files_get_filesystem
                namespace: company.team

                tasks:
                  - id: get_fs
                    type: io.kestra.plugin.aws.s3.files.GetFileSystem
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "us-east-1"
                    fileSystemId: "fs-0123456789abcdef0"
                """
        )
    }
)
@Schema(
    title = "Get an Amazon S3 Files file system",
    description = "Retrieves the metadata and status of an existing S3 Files file system."
)
public class GetFileSystem extends AbstractS3Files implements RunnableTask<GetFileSystem.Output> {

    @Schema(title = "File system ID", description = "The ID of the file system to retrieve (e.g. fs-0123456789abcdef0).")
    @NotNull
    private Property<String> fileSystemId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String fsId = runContext.render(fileSystemId).as(String.class).orElseThrow();

        S3FilesService.Response response = executeRequest(
            runContext,
            SdkHttpMethod.GET,
            "/filesystems/" + fsId,
            null
        );

        FileSystem fs = S3FilesService.fromJson(response.body(), FileSystem.class);

        runContext.logger().info("Retrieved S3 Files file system: {} (status={})", fs.getFileSystemId(), fs.getStatus());

        return Output.builder()
            .fileSystem(fs)
            .build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    @Schema(title = "Output of the GetFileSystem task")
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "The full file system details")
        private FileSystem fileSystem;
    }
}

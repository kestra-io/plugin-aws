package io.kestra.plugin.aws.s3.files;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.http.SdkHttpMethod;

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
                id: aws_s3_files_delete_filesystem
                namespace: company.team

                tasks:
                  - id: delete_fs
                    type: io.kestra.plugin.aws.s3.files.DeleteFileSystem
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "us-east-1"
                    fileSystemId: "fs-0123456789abcdef0"
                """
        )
    }
)
@Schema(
    title = "Delete an Amazon S3 Files file system",
    description = "Deletes an S3 Files file system. All mount targets must be removed before issuing this call."
)
public class DeleteFileSystem extends AbstractS3Files implements RunnableTask<DeleteFileSystem.Output> {

    @Schema(title = "File system ID", description = "The ID of the file system to delete.")
    @NotNull
    private Property<String> fileSystemId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String fsId = runContext.render(fileSystemId).as(String.class).orElseThrow();

        executeRequest(runContext, SdkHttpMethod.DELETE, "/filesystems/" + fsId, null);

        runContext.logger().info("Deleted S3 Files file system: {}", fsId);

        return Output.builder().build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    @Schema(title = "Output of the DeleteFileSystem task")
    public static class Output implements io.kestra.core.models.tasks.Output {
    }
}

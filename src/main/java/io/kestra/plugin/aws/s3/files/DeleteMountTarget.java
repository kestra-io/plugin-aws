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

/**
 * Deletes an Amazon S3 Files mount target.
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
                id: aws_s3_files_delete_mount_target
                namespace: company.team

                tasks:
                  - id: delete_mt
                    type: io.kestra.plugin.aws.s3.files.DeleteMountTarget
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "us-east-1"
                    mountTargetId: "mt-0123456789abcdef0"
                """
        )
    }
)
@Schema(
    title = "Delete an Amazon S3 Files mount target",
    description = "Deletes the specified NFS mount target. The file system becomes inaccessible from the associated subnet once the mount target is deleted."
)
public class DeleteMountTarget extends AbstractS3Files implements RunnableTask<DeleteMountTarget.Output> {

    @Schema(title = "Mount target ID", description = "The ID of the mount target to delete.")
    @NotNull
    private Property<String> mountTargetId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String mtId = runContext.render(mountTargetId).as(String.class).orElseThrow();

        executeRequest(runContext, SdkHttpMethod.DELETE, "/mounttargets/" + mtId, null);

        runContext.logger().info("Deleted S3 Files mount target: {}", mtId);

        return Output.builder().build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    @Schema(title = "Output of the DeleteMountTarget task")
    public static class Output implements io.kestra.core.models.tasks.Output {
    }
}

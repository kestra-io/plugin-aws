package io.kestra.plugin.aws.s3.files;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.s3.files.models.FileSystem;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.http.SdkHttpMethod;

/**
 * Creates an Amazon S3 Files file system backed by an S3 bucket.
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
                id: aws_s3_files_create_filesystem
                namespace: company.team

                tasks:
                  - id: create_fs
                    type: io.kestra.plugin.aws.s3.files.CreateFileSystem
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "us-east-1"
                    bucket: "arn:aws:s3:::my-bucket"
                    roleArn: "arn:aws:iam::123456789012:role/S3FilesRole"
                """
        )
    }
)
@Schema(
    title = "Create an Amazon S3 Files file system",
    description = "Creates an S3 Files file system resource that makes an S3 bucket mountable as an NFS v4.1+ file system."
)
public class CreateFileSystem extends AbstractS3Files implements RunnableTask<CreateFileSystem.Output> {

    @Schema(title = "S3 bucket ARN", description = "ARN of the S3 bucket to back the file system (e.g. arn:aws:s3:::my-bucket).")
    @NotNull
    private Property<String> bucket;

    @Schema(title = "IAM role ARN", description = "ARN of the IAM role that grants the S3 Files service access to the bucket.")
    @NotNull
    private Property<String> roleArn;

    @Schema(title = "Key prefix", description = "Optional prefix scoping the file system to a sub-path of the bucket.")
    private Property<String> prefix;

    @Schema(title = "KMS key ID", description = "ARN, key ID, or alias of the KMS key for encryption. When omitted the service-owned key is used.")
    private Property<String> kmsKeyId;

    @Schema(title = "Client token", description = "Idempotency token (up to 64 ASCII characters). Automatically generated when omitted.")
    private Property<String> clientToken;

    @Schema(title = "Tags", description = "Key-value tags to apply to the file system.")
    @PluginProperty(dynamic = false)
    private Property<Map<String, String>> tags;

    @Schema(title = "Accept bucket warning", description = "Set to true to acknowledge any bucket configuration warnings and proceed with creation.")
    @Builder.Default
    private Property<Boolean> acceptBucketWarning = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put("bucket", runContext.render(bucket).as(String.class).orElseThrow());
        body.put("roleArn", runContext.render(roleArn).as(String.class).orElseThrow());

        runContext.render(prefix).as(String.class).ifPresent(v -> body.put("prefix", v));
        runContext.render(kmsKeyId).as(String.class).ifPresent(v -> body.put("kmsKeyId", v));
        runContext.render(clientToken).as(String.class).ifPresent(v -> body.put("clientToken", v));
        if (Boolean.TRUE.equals(runContext.render(acceptBucketWarning).as(Boolean.class).orElse(false))) {
            body.put("acceptBucketWarning", true);
        }

        if (tags != null) {
            Map<String, String> tagMap = runContext.render(tags).asMap(String.class, String.class);
            if (!tagMap.isEmpty()) {
                List<Map<String, String>> tagList = tagMap.entrySet().stream()
                    .map(e -> Map.of("key", e.getKey(), "value", e.getValue()))
                    .toList();
                body.put("tags", tagList);
            }
        }

        S3FilesService.Response response = executeRequest(
            runContext,
            SdkHttpMethod.POST,
            "/filesystems",
            S3FilesService.toJson(body)
        );

        FileSystem fs = S3FilesService.fromJson(response.body(), FileSystem.class);

        runContext.logger().info("Created S3 Files file system: {} (status={})", fs.getFileSystemId(), fs.getStatus());

        return Output.builder()
            .fileSystemId(fs.getFileSystemId())
            .fileSystemArn(fs.getFileSystemArn())
            .status(fs.getStatus())
            .creationTime(fs.getCreationTime())
            .build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    @Schema(title = "Output of the CreateFileSystem task")
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "File system ID")
        private String fileSystemId;

        @Schema(title = "File system ARN")
        private String fileSystemArn;

        @Schema(title = "Lifecycle status")
        private String status;

        @Schema(title = "Creation time (Unix epoch seconds)")
        private Long creationTime;
    }
}

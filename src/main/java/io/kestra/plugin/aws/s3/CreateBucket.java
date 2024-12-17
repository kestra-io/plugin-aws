package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Create a new bucket with some options",
            full = true,
            code = """
                id: aws_s3_create_bucket
                namespace: company.team

                tasks:
                  - id: create_bucket
                    type: io.kestra.plugin.aws.s3.CreateBucket
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    bucket: "my-bucket"
            """
        )
    }
)
@Schema(
    title = "Create a bucket"
)
public class CreateBucket extends AbstractConnection implements AbstractS3, RunnableTask<CreateBucket.Output> {
    @Schema(
        description = "The S3 bucket name to create."
    )
    @NotNull
    private Property<String> bucket;

    @Schema(
        description = "Allows grantee the read, write, read ACP, and write ACP permissions on the bucket."
    )
    private Property<String> grantFullControl;

    @Schema(
        title = "Allows grantee to list the objects in the bucket."
    )
    private Property<String> grantRead;

    @Schema(
        title = "Allows grantee to list the ACL for the applicable bucket."
    )
    private Property<String> grantReadACP;

    @Schema(
        title = "Allows grantee to create, overwrite, and delete any object in the bucket."
    )
    private Property<String> grantWrite;

    @Schema(
        title = "Allows grantee to write the ACL for the applicable bucket."
    )
    private Property<String> grantWriteACP;

    @Schema(
        title = "The canned ACL to apply to the bucket."
    )
    private Property<String> acl;

    @Schema(
        title = "Specifies whether you want S3 Object Lock to be enabled for the new bucket."
    )
    private Property<Boolean> objectLockEnabledForBucket;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket).as(String.class).orElseThrow();

        try (S3Client client = this.client(runContext)) {
            CreateBucketRequest.Builder builder = CreateBucketRequest.builder()
                .bucket(bucket);

            if (grantFullControl != null) {
                builder.grantFullControl(runContext.render(this.grantFullControl).as(String.class).orElseThrow());
            }

            if (grantRead != null) {
                builder.grantRead(runContext.render(this.grantRead).as(String.class).orElseThrow());
            }

            if (grantReadACP != null) {
                builder.grantReadACP(runContext.render(this.grantReadACP).as(String.class).orElseThrow());
            }

            if (grantWrite != null) {
                builder.grantWrite(runContext.render(this.grantWrite).as(String.class).orElseThrow());
            }


            if (grantWriteACP != null) {
                builder.grantWriteACP(runContext.render(this.grantWriteACP).as(String.class).orElseThrow());
            }

            if (acl != null) {
                builder.acl(runContext.render(this.acl).as(String.class).orElseThrow());
            }

            if (objectLockEnabledForBucket != null) {
                builder.objectLockEnabledForBucket(runContext.render(this.objectLockEnabledForBucket).as(Boolean.class).orElseThrow());
            }

            CreateBucketResponse response = client.createBucket(builder.build());

            return Output
                .builder()
                .bucket(bucket)
                .region(response.location())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private final String bucket;
        private final String region;
    }
}

package org.kestra.task.aws.s3;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    title = "Create a new bucket with some options",
    code = {
        "name: \"my-bucket\"",
        "region: \"eu-west-1\""
    }
)
@Documentation(
    description = "Create a bucket"
)
public class CreateBucket extends AbstractS3 implements RunnableTask<CreateBucket.Output> {
    @InputProperty(
        description = "The bucket where to download the file",
        dynamic = true
    )
    private String bucket;


    @InputProperty(
        description = "Allows grantee the read, write, read ACP, and write ACP permissions on the bucket.",
        dynamic = true
    )
    private String grantFullControl;

    @InputProperty(
        description = "Allows grantee to list the objects in the bucket.",
        dynamic = true
    )
    private String grantRead;

    @InputProperty(
        description = "Allows grantee to list the objects in the bucket.",
        dynamic = true
    )
    private String grantReadACP;

    @InputProperty(
        description = "Allows grantee to create, overwrite, and delete any object in the bucket.",
        dynamic = true
    )
    private String grantWrite;

    @InputProperty(
        description = "Allows grantee to write the ACL for the applicable bucket.",
        dynamic = true
    )
    private String grantWriteACP;

    @InputProperty(
        description = "The canned ACL to apply to the bucket.",
        dynamic = true
    )
    private String acl;

    @InputProperty(
        description = "Specifies whether you want S3 Object Lock to be enabled for the new bucket."
    )
    private Boolean objectLockEnabledForBucket;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket);

        try (S3Client client = this.client(runContext)) {
            CreateBucketRequest.Builder builder = CreateBucketRequest.builder()
                .bucket(bucket);

            if (grantFullControl != null) {
                builder.grantFullControl(runContext.render(this.grantFullControl));
            }

            if (grantRead != null) {
                builder.grantRead(runContext.render(this.grantRead));
            }

            if (grantReadACP != null) {
                builder.grantReadACP(runContext.render(this.grantReadACP));
            }

            if (grantWrite != null) {
                builder.grantWrite(runContext.render(this.grantWrite));
            }


            if (grantWriteACP != null) {
                builder.grantWriteACP(runContext.render(this.grantWriteACP));
            }

            if (acl != null) {
                builder.acl(runContext.render(this.acl));
            }

            if (objectLockEnabledForBucket != null) {
                builder.objectLockEnabledForBucket(this.objectLockEnabledForBucket);
            }

            if (objectLockEnabledForBucket != null) {
                builder.objectLockEnabledForBucket(this.objectLockEnabledForBucket);
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
    public static class Output implements org.kestra.core.models.tasks.Output {
        private final String bucket;
        private final String region;
    }
}

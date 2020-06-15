package org.kestra.task.aws.s3;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;

import java.io.File;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    code = {
        "bucket: \"my-bucket\"",
        "key: \"path/to/file\""
    }
)
@Documentation(
    description = "Download a file to a S3 bucket."
)
public class Delete extends AbstractS3 implements RunnableTask<Delete.Output> {
    @InputProperty(
        description = "The bucket",
        dynamic = true
    )
    private String bucket;

    @InputProperty(
        description = "The key to delete",
        dynamic = true
    )
    private String key;

    @InputProperty(
        description = "Indicates whether S3 Object Lock should bypass Governance-mode restrictions to process this operation."
    )
    private Boolean bypassGovernanceRetention;

    @InputProperty(
        description = "The concatenation of the authentication device's serial number, a space, and the value that is displayed on " +
            "your authentication device. Required to permanently delete a versioned object if versioning is configured " +
            "with MFA delete enabled.",
        dynamic = true
    )
    private String mfa;

    @InputProperty(
        description = "Sets the value of the RequestPayer property for this object.",
        dynamic = true
    )
    private String requestPayer;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket);
        String key = runContext.render(this.key);
        File tempFile = File.createTempFile("download_", ".s3");
        //noinspection ResultOfMethodCallIgnored
        tempFile.delete();

        try (S3Client client = this.client(runContext)) {
            DeleteObjectRequest.Builder builder = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(key);

            if (this.bypassGovernanceRetention != null) {
                builder.bypassGovernanceRetention(this.bypassGovernanceRetention);
            }

            if (this.mfa != null) {
                builder.mfa(runContext.render(this.mfa));
            }

            if (this.requestPayer != null) {
                builder.requestPayer(runContext.render(this.requestPayer));
            }

            DeleteObjectResponse response = client.deleteObject(builder.build());

            return Output
                .builder()
                .versionId(response.versionId())
                .deleteMarker(response.deleteMarker())
                .requestCharged(response.requestChargedAsString())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "Returns the version ID of the delete marker created as a result of the DELETE operation."
        )
        private final String versionId;

        @OutputProperty(
            description = "Specifies whether the versioned object that was permanently deleted was (true) or was not (false) a delete marker."
        )
        private final Boolean deleteMarker;

        @OutputProperty(
            description = "Returns the value of the RequestCharged property for this object."
        )
        private final String requestCharged;

    }
}

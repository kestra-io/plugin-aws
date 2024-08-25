package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;

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
                id: aws_s3_delete
                namespace: company.name

                tasks:
                  - id: delete
                    type: io.kestra.plugin.aws.s3.Delete
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    key: "path/to/file"
                """
        )
    }
)
@Schema(
    title = "Delete a file in a S3 bucket."
)
public class Delete extends AbstractS3Object implements RunnableTask<Delete.Output> {
    @Schema(
        title = "The key to delete."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String key;

    @Schema(
        title = "Indicates whether S3 Object Lock should bypass Governance-mode restrictions to process this operation."
    )
    @PluginProperty
    private Boolean bypassGovernanceRetention;

    @Schema(
        title = "The concatenation of the authentication device's serial number, a space, and the value that is displayed on " +
            "your authentication device.",
        description = "Required to permanently delete a versioned object if versioning is configured " +
            "with MFA delete enabled."
    )
    @PluginProperty(dynamic = true)
    private String mfa;

    @Schema(
        description = "Sets the value of the RequestPayer property for this object."
    )
    @PluginProperty(dynamic = true)
    private String requestPayer;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket);
        String key = runContext.render(this.key);

        try (S3Client client = client(runContext)) {
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
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Returns the version ID of the delete marker created as a result of the DELETE operation."
        )
        private final String versionId;

        @Schema(
            title = "Specifies whether the versioned object that was permanently deleted was (true) or was not (false) a delete marker."
        )
        private final Boolean deleteMarker;

        @Schema(
            title = "Returns the value of the RequestCharged property for this object."
        )
        private final String requestCharged;

    }
}

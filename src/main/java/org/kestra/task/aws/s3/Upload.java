package org.kestra.task.aws.s3;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    code = {
        "from: \"{{ inputs.file }}\"",
        "bucket: \"my-bucket\"",
        "key: \"path/to/file\""
    }
)
@Documentation(
    description = "Upload a file to a S3 bucket."
)
public class Upload extends AbstractS3Object implements RunnableTask<Upload.Output> {
    @InputProperty(
        description = "The file to upload",
        dynamic = true
    )
    private String from;

    @InputProperty(
        description = "The bucket where to upload the file",
        dynamic = true
    )
    private String bucket;

    @InputProperty(
        description = "The key where to upload the file",
        dynamic = true
    )
    private String key;

    @InputProperty(
        description = "A map of metadata to store with the object in S3."
    )
    private Map<String, String> metadata;

    @InputProperty(
        description = "If you don't specify, S3 Standard is the default storage class. Amazon S3 supports other storage classes.",
        dynamic = true
    )
    private String storageClass;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket);
        String key = runContext.render(this.key);

        try (S3Client client = this.client(runContext)) {
            File tempFile = File.createTempFile("upload_", ".s3");
            URI from = new URI(runContext.render(this.from));
            Files.copy(runContext.uriToInputStream(from), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            PutObjectRequest.Builder builder = PutObjectRequest
                .builder()
                .bucket(bucket)
                .key(key);

            if (this.requestPayer != null) {
                builder.requestPayer(runContext.render(this.requestPayer));
            }

            if (this.metadata != null) {
                builder.metadata(this.metadata);
            }

            if (this.storageClass != null) {
                builder.storageClass(runContext.render(this.storageClass));
            }

            PutObjectResponse response = client.putObject(
                builder.build(),
                RequestBody.fromFile(tempFile)
            );
            runContext.metric(Counter.of("file.size", tempFile.length()));

            //noinspection ResultOfMethodCallIgnored
            tempFile.delete();


            return Output
                .builder()
                .bucket(bucket)
                .key(key)
                .eTag(response.eTag())
                .versionId(response.versionId())
                .build();
        }

    }

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements org.kestra.core.models.tasks.Output {
        private final String bucket;
        private final String key;
    }
}

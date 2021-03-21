package io.kestra.plugin.aws.s3;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractS3 extends AbstractConnection {
    @Schema(
        title = "The AWS region to used"
    )
    @PluginProperty(dynamic = true)
    private String region;

    S3Client client(RunContext runContext) throws IllegalVariableEvaluationException {
        S3ClientBuilder s3ClientBuilder = S3Client.builder()
            .credentialsProvider(this.credentials(runContext));

        String region = runContext.render(this.region);
        if (this.region != null) {
            s3ClientBuilder.region(Region.of(region));
        }

        return s3ClientBuilder.build();
    }
}

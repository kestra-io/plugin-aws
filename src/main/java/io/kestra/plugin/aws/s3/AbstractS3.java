package io.kestra.plugin.aws.s3;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractS3 extends AbstractConnection implements AbstractS3Interface  {
    protected String region;

    protected String endpointOverride;

    protected Boolean pathStyleAccess;

    protected S3Client client(RunContext runContext) throws IllegalVariableEvaluationException {
        S3ClientBuilder s3ClientBuilder = S3Client.builder()
            .httpClient(ApacheHttpClient.create())
            .credentialsProvider(this.credentials(runContext));

        if (this.region != null) {
            s3ClientBuilder.region(Region.of(runContext.render(this.region)));
        }

        if (this.endpointOverride != null) {
            s3ClientBuilder.endpointOverride(URI.create(runContext.render(this.endpointOverride)));
        }

        if (this.pathStyleAccess != null) {
            s3ClientBuilder.serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(this.pathStyleAccess)
                .build()
            );
        }

        return s3ClientBuilder.build();
    }
}

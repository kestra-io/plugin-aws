package io.kestra.plugin.aws.s3;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.*;

import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractS3 extends AbstractConnection {

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

        return s3ClientBuilder.build();
    }

    protected S3AsyncClient asyncClient(RunContext runContext) throws IllegalVariableEvaluationException {

        if (this.getUseDefaultAsyncClient()) {
            S3AsyncClientBuilder s3ClientBuilder = S3AsyncClient.builder()
                .credentialsProvider(this.credentials(runContext));

            if (this.region != null) {
                s3ClientBuilder.region(Region.of(runContext.render(this.region)));
            }

            if (this.endpointOverride != null) {
                s3ClientBuilder.endpointOverride(URI.create(runContext.render(this.endpointOverride)));
            }
            return s3ClientBuilder.build();

        } else {
            S3CrtAsyncClientBuilder s3ClientBuilder = S3AsyncClient.crtBuilder()
                .credentialsProvider(this.credentials(runContext));

            if (this.region != null) {
                s3ClientBuilder.region(Region.of(runContext.render(this.region)));
            }

            if (this.endpointOverride != null) {
                s3ClientBuilder.endpointOverride(URI.create(runContext.render(this.endpointOverride)));
            }

            return s3ClientBuilder.build();
        }

    }
}

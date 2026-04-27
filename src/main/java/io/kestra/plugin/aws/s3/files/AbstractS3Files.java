package io.kestra.plugin.aws.s3.files;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractS3Files extends AbstractConnection {

    private static final String S3_FILES_SERVICE = "s3files";
    private static final ApacheHttpClient HTTP_CLIENT = (ApacheHttpClient) ApacheHttpClient.create();

    protected AwsCredentialsProvider credentialsProvider(AbstractConnection.AwsClientConfig cfg) {
        if (cfg.stsRoleArn() != null) {
            return stsCredentialsProvider(cfg);
        }

        if (cfg.accessKeyId() != null && cfg.secretKeyId() != null) {
            if (cfg.sessionToken() != null) {
                return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(cfg.accessKeyId(), cfg.secretKeyId(), cfg.sessionToken())
                );
            }
            return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(cfg.accessKeyId(), cfg.secretKeyId())
            );
        }

        return DefaultCredentialsProvider.create();
    }

    private AwsCredentialsProvider stsCredentialsProvider(AbstractConnection.AwsClientConfig cfg) {
        StsClientBuilder stsBuilder = StsClient.builder()
            .region(Region.of(cfg.region()));

        if (cfg.stsEndpointOverride() != null) {
            stsBuilder.endpointOverride(URI.create(cfg.stsEndpointOverride()));
        }

        if (cfg.accessKeyId() != null && cfg.secretKeyId() != null) {
            stsBuilder.credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(cfg.accessKeyId(), cfg.secretKeyId())
                )
            );
        }

        try (StsClient stsClient = stsBuilder.build()) {
            AssumeRoleRequest.Builder req = AssumeRoleRequest.builder()
                .roleArn(cfg.stsRoleArn())
                .roleSessionName(
                    cfg.stsRoleSessionName() != null
                        ? cfg.stsRoleSessionName()
                        : "kestra-s3files-session"
                )
                .durationSeconds(
                    (int) (cfg.stsRoleSessionDuration() != null
                        ? cfg.stsRoleSessionDuration().getSeconds()
                        : Duration.ofMinutes(15).getSeconds())
                );

            if (cfg.stsRoleExternalId() != null) {
                req.externalId(cfg.stsRoleExternalId());
            }

            Credentials c = stsClient.assumeRole(req.build()).credentials();
            return StaticCredentialsProvider.create(
                AwsSessionCredentials.create(c.accessKeyId(), c.secretAccessKey(), c.sessionToken())
            );
        }
    }

    // HTTP request execution

    /**
     *
     * @param runContext Kestra run context used for rendering properties
     * @param method HTTP method (GET, PUT, POST, DELETE)
     * @param path API path, e.g. {@code "/file-systems"} or
     *        {@code "/file-systems/fs-abc/mount-targets"}
     * @param body request body bytes; {@code null} for bodyless methods (GET/DELETE)
     * @return {@link S3FilesService.Response} containing the HTTP status and raw JSON body
     * @throws RuntimeException if the API returns a non-2xx status code
     */
    protected S3FilesService.Response executeRequest(
        RunContext runContext,
        SdkHttpMethod method,
        String path,
        byte[] body) throws Exception {

        AbstractConnection.AwsClientConfig cfg = this.awsClientConfig(runContext);

        String regionStr = cfg.region();
        if (regionStr == null) {
            throw new IllegalArgumentException("region is required for S3 Files tasks");
        }
        Region region = Region.of(regionStr);

        String serviceHost = S3_FILES_SERVICE + "." + regionStr + ".amazonaws.com";

        String baseUrl;
        String hostHeader;
        if (cfg.endpointOverride() != null) {
            baseUrl = cfg.endpointOverride().replaceAll("/$", "");
            URI overrideUri = URI.create(cfg.endpointOverride());
            hostHeader = overrideUri.getPort() > 0
                ? overrideUri.getHost() + ":" + overrideUri.getPort()
                : overrideUri.getHost();
        } else {
            baseUrl = "https://" + serviceHost;
            hostHeader = serviceHost;
        }

        URI requestUri = URI.create(baseUrl + path);

        byte[] bodyBytes = (body != null) ? body : new byte[0];

        SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
            .method(method)
            .uri(requestUri)
            .putHeader("Content-Type", "application/json")
            .putHeader("Host", hostHeader);

        if (bodyBytes.length > 0) {
            requestBuilder.contentStreamProvider(() -> new ByteArrayInputStream(bodyBytes));
        }

        SdkHttpFullRequest unsignedRequest = requestBuilder.build();

        Aws4SignerParams signerParams = Aws4SignerParams.builder()
            .awsCredentials(credentialsProvider(cfg).resolveCredentials())
            .signingName(S3_FILES_SERVICE)
            .signingRegion(region)
            .build();

        SdkHttpFullRequest signedRequest = Aws4Signer.create().sign(unsignedRequest, signerParams);

        ExecutableHttpRequest executableRequest = HTTP_CLIENT.prepareRequest(
            HttpExecuteRequest.builder()
                .request(signedRequest)
                .contentStreamProvider(
                    bodyBytes.length > 0
                        ? () -> new ByteArrayInputStream(bodyBytes)
                        : null
                )
                .build()
        );

        HttpExecuteResponse response = executableRequest.call();
        int statusCode = response.httpResponse().statusCode();

        String responseBody = "";
        if (response.responseBody().isPresent()) {
            try (InputStream is = response.responseBody().get()) {
                responseBody = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
        }

        if (statusCode < 200 || statusCode >= 300) {
            throw new RuntimeException(
                "S3 Files API error [HTTP " + statusCode + "]: " + responseBody
            );
        }

        return new S3FilesService.Response(statusCode, responseBody);
    }
}

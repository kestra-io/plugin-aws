package io.kestra.plugin.aws.auth;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.EncryptedString;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Base64;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Generate a presigned EKS authentication token",
    description = "Builds a short-lived `k8s-aws-v1` token for a given EKS cluster by presigning STS GetCallerIdentity. Requires region and cluster name; expirationDuration defaults to 600s."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: aws_eks_oauth_token
                namespace: company.team

                tasks:
                  - id: get_eks_token
                    type: io.kestra.plugin.aws.auth.EksToken
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    clusterName: "my-cluster"
                """
        )
    }
)
public class EksToken extends AbstractConnection implements RunnableTask<EksToken.Output> {

    @Schema(
        title = "EKS cluster name",
        description = "Cluster identifier passed in x-k8s-aws-id when presigning."
    )
    @NotNull
    private Property<String> clusterName;

    @Schema(
        title = "Token TTL (seconds)",
        description = "Lifetime of the presigned URL; default 600 seconds."
    )
    @NotNull
    @Builder.Default
    private Property<Long> expirationDuration = Property.ofValue(600L);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try {
            if(this.getRegion() == null) {
                throw new RuntimeException("Region is required");
            }
            final Region awsRegion = Region.of(runContext.render(this.getRegion()).as(String.class).orElseThrow());

            SdkHttpFullRequest requestToSign = SdkHttpFullRequest
                .builder()
                .method(SdkHttpMethod.GET)
                .uri(getStsRegionalEndpointUri(runContext, awsRegion))
                .appendHeader("x-k8s-aws-id", runContext.render(this.clusterName).as(String.class).orElseThrow())
                .appendRawQueryParameter("Action", "GetCallerIdentity")
                .appendRawQueryParameter("Version", "2011-06-15")
                .build();

            ZonedDateTime expirationDate = ZonedDateTime.now().plusSeconds(runContext.render(expirationDuration).as(Long.class).orElseThrow());
            Aws4PresignerParams presignerParams = Aws4PresignerParams.builder()
                .awsCredentials(ConnectionUtils.credentialsProvider(this.awsClientConfig(runContext)).resolveCredentials())
                .signingRegion(awsRegion)
                .signingName("sts")
                .signingClockOverride(Clock.systemUTC())
                .expirationTime(expirationDate.toInstant())
                .build();

            SdkHttpFullRequest signedRequest = Aws4Signer.create().presign(requestToSign, presignerParams);

            String encodedUrl = Base64.getUrlEncoder().withoutPadding().encodeToString(signedRequest.getUri().toString().getBytes(StandardCharsets.UTF_8));

            var token = Token.builder()
                .expirationTime(expirationDate.toInstant())
                .tokenValue(EncryptedString.from("k8s-aws-v1." + encodedUrl, runContext))
                .build();

            return Output.builder()
                .token(token)
                .build();
        } catch (Exception e) {
            String errorMessage = "A problem occurred generating an Eks authentication token for cluster: " + clusterName;
            runContext.logger().error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    public static URI getStsRegionalEndpointUri(RunContext runContext, Region awsRegion) {
        try {
            return new URI("https", String.format("sts.%s.amazonaws.com", awsRegion.id()), "/", null);
        } catch (URISyntaxException e) {
            String errorMessage = "An error occurred creating the STS regional endpoint Uri";
            runContext.logger().error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @NotNull
        @Schema(
            title = "EKS auth token",
            description = "Bearer token formatted as k8s-aws-v1.<base64url>; encrypted in outputs when supported."
        )
        private final Token token;
    }

    @Builder
    @Getter
    public static class Token {
        @Schema(
            title = "OAuth access token value",
            description = "Will be automatically encrypted and decrypted in the outputs if encryption is configured"
        )
        EncryptedString tokenValue;

        @Schema(
            title = "Token expiration time",
            description = "Exact UTC expiration timestamp derived from the provided TTL."
        )
        Instant expirationTime;
    }
}

package io.kestra.plugin.aws.ecr;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.EcrClientBuilder;
import software.amazon.awssdk.services.ecr.model.AuthorizationData;

import java.net.URI;
import java.util.Base64;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Retrieve AWS ECR authorization token to push Docker images to Amazon ECR and to pull images from that container registry."
)
@Plugin(
        examples = {
                @Example(
                        title = "Retrieve the AWS ECR authorization token",
                        code = {
                                "accessKeyId: \"<access-key>\"",
                                "secretKeyId: \"<secret-key>\"",
                                "region: \"eu-central-1\""
                        }
                ),
        }
)
public class GetAuthToken extends AbstractConnection implements RunnableTask<GetAuthToken.TokenOutput> {

    @Override
    public TokenOutput run(RunContext runContext) throws Exception {
        EcrClientBuilder ecrClientBuilder = EcrClient.builder().credentialsProvider(this.credentials(runContext));

        if (this.region != null) {
            ecrClientBuilder.region(Region.of(runContext.render(this.region)));
        }

        if (this.endpointOverride != null) {
            ecrClientBuilder.endpointOverride(URI.create(runContext.render(this.endpointOverride)));
        }

        try (EcrClient client = ecrClientBuilder.build()) {
            List<AuthorizationData> authorizationData = client.getAuthorizationToken().authorizationData();

            String encodedToken = authorizationData.get(0).authorizationToken();
            byte[] decodedTokenBytes = Base64.getDecoder().decode(encodedToken);

            String token = new String(decodedTokenBytes);
            if (token.indexOf(":") > 0) {
                token = token.substring(token.indexOf(":") + 1);
            }

            return TokenOutput.builder()
                .token(token)
                .build();
        }
    }

    @Builder
    @Getter
    public static class TokenOutput implements Output {

        @Schema(title = "Amazon authorization token")
        private String token;

    }
}

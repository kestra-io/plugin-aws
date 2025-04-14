package io.kestra.plugin.aws.ecr;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.EncryptedString;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.AuthorizationData;

import java.util.Base64;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Retrieve an AWS ECR token to push or pull Docker images."
)
@Plugin(
    examples = {
        @Example(
            title = "Retrieve the AWS ECR authorization token.",
            full = true,
            code = """
                id: aws_ecr_get_auth_token
                namespace: company.team

                tasks:
                  - id: get_auth_token
                    type: io.kestra.plugin.aws.ecr.GetAuthToken
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                """
        )
    }
)
public class GetAuthToken extends AbstractConnection implements RunnableTask<GetAuthToken.TokenOutput> {

    @Override
    public TokenOutput run(RunContext runContext) throws Exception {
        try (EcrClient client = client(runContext)) {
            List<AuthorizationData> authorizationData = client.getAuthorizationToken().authorizationData();

            String encodedToken = authorizationData.get(0).authorizationToken();
            byte[] decodedTokenBytes = Base64.getDecoder().decode(encodedToken);

            String token = new String(decodedTokenBytes);
            if (token.indexOf(":") > 0) {
                token = token.substring(token.indexOf(":") + 1);
            }

            return TokenOutput.builder()
                .token(EncryptedString.from(token, runContext))
                .build();
        }
    }

    private EcrClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, EcrClient.builder()).build();
    }

    @Builder
    @Getter
    public static class TokenOutput implements Output {

        @Schema(
            title = "AWS ECR authorization token.",
            description = "Will be automatically encrypted and decrypted in the outputs if encryption is configured"
        )
        private EncryptedString token;

    }
}

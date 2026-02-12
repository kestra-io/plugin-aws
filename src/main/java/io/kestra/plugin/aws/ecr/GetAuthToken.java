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
    title = "Get an ECR authorization token",
    description = "Fetches a short-lived Basic auth password for Docker push/pull to Amazon ECR by calling GetAuthorizationToken. Returns only the password portion (username is always AWS)."
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
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
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

    protected EcrClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, EcrClient.builder()).build();
    }

    @Builder
    @Getter
    public static class TokenOutput implements Output {

        @Schema(
            title = "ECR password token",
            description = "Password extracted from the Base64 Basic auth token; encrypted in outputs when supported."
        )
        private EncryptedString token;

    }
}

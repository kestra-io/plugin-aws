package io.kestra.plugin.aws.lambda;

import java.net.URI;
import javax.validation.constraints.NotNull;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLambdaInvoke extends AbstractConnection {
    @Schema(title = "The Lambda function name.")
    @PluginProperty(dynamic = true)
    @NotNull
    protected String functionArn;

    /**
     * Currently we assume synchronous invokation of Lambda function.
     */
    protected final Boolean wait = true;

    protected String customAccessKeyId;

    protected String customSecretKeyId;

    protected String customRegion;

    protected LambdaClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        LambdaClientBuilder builder = LambdaClient.builder().httpClient(ApacheHttpClient.create())
                .credentialsProvider(this.credentials(runContext));

        if (this.region != null) {
            builder.region(Region.of(runContext.render(this.region)));
        }
        if (this.endpointOverride != null) {
            builder.endpointOverride(URI.create(runContext.render(this.endpointOverride)));
        }

        return builder.build();
    }

}

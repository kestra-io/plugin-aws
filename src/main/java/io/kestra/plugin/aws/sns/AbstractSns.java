package io.kestra.plugin.aws.sns;

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
import software.amazon.awssdk.services.sns.SnsClient;

import java.net.URI;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractSns  extends AbstractConnection {
    @Schema(title = "The SNS topic ARN. The topic must already exist.")
    @PluginProperty(dynamic = true)
    @NotNull
    private String topicArn;

    protected SnsClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        var builder = SnsClient.builder()
            .httpClient(ApacheHttpClient.create())
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

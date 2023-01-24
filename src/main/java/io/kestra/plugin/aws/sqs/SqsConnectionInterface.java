package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface SqsConnectionInterface extends AbstractConnectionInterface {
    @Schema(title = "The SQS queue URL. The queue must already exist.")
    @PluginProperty(dynamic = true)
    @NotNull
    String getQueueUrl();
}

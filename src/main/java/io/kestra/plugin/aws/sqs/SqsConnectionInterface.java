package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface SqsConnectionInterface extends AbstractConnectionInterface {
    @Schema(title = "The SQS queue URL. The queue must already exist.")
    @NotNull
    Property<String> getQueueUrl();
}

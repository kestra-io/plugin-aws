package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public interface SqsConnectionInterface extends AbstractConnectionInterface {
    @Schema(title = "The SQS queue URL. The queue must already exist.")
    @NotNull
    Property<String> getQueueUrl();

    @Schema(
        title = "Maximum number of allowed concurrent requests.",
        description = "For HTTP/1.1 this is the same as max connections. For HTTP/2 the number of connections that will be used depends on the max streams allowed per connection.\n" +
            "If the maximum number of concurrent requests is exceeded they may be queued in the HTTP client (see `maxPendingConnectionAcquires`) and can cause increased latencies. If the client is overloaded enough such that the pending connection queue fills up, subsequent requests may be rejected or time out (see `connectionAcquisitionTimeout`)."
    )
    Property<Integer> getMaxConcurrency();

    @Schema(title = "The amount of time to wait when acquiring a connection from the pool before giving up and timing out.")
    Property<Duration> getConnectionAcquisitionTimeout();
}

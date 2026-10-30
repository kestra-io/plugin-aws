package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.sqs.model.PurgeQueueInProgressException;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Purge all messages from an SQS queue",
    description = """
        Deletes every message in the queue, including messages that are currently in flight, with a single PurgeQueue call.
        Deleted messages cannot be recovered: consume or redrive anything you still need before purging.
        AWS allows one purge per queue every 60 seconds: a second call inside that window fails with `PurgeQueueInProgressException` and this task fails with a message that says so.
        The purge itself runs asynchronously on the AWS side and can take up to 60 seconds to finish; messages sent during that window may be deleted as well."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Purge a dead-letter queue before replaying a batch",
            code = """
                id: aws_sqs_purge
                namespace: company.team

                tasks:
                  - id: purge_dlq
                    type: io.kestra.plugin.aws.sqs.Purge
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                    region: "{{ secret('AWS_DEFAULT_REGION') }}"
                    queueUrl: "https://sqs.eu-central-1.amazonaws.com/000000000000/orders-dlq"
                """
        )
    }
)
public class Purge extends AbstractSqs implements RunnableTask<Purge.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rQueueUrl = runContext.render(getQueueUrl()).as(String.class)
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("The `queueUrl` property is required and rendered to an empty value; set it to the full SQS queue URL."));

        try (var sqsClient = this.client(runContext)) {
            try {
                sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(rQueueUrl).build());
            } catch (PurgeQueueInProgressException e) {
                throw new IllegalStateException(
                    "A purge of queue '" + rQueueUrl + "' is already in progress. AWS allows one PurgeQueue call per queue every 60 seconds; retry once the previous purge has completed.",
                    e
                );
            }

            runContext.logger().info("Purge requested for SQS queue '{}'. AWS can take up to 60 seconds to delete all messages.", rQueueUrl);

            return Output.builder()
                .queueUrl(rQueueUrl)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Purged queue URL",
            description = "URL of the queue the purge was requested on."
        )
        private final String queueUrl;
    }
}

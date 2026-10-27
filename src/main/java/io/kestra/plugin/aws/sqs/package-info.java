@PluginSubGroup(
    title = "SQS",
    description = "Tasks that publish to, consume from, or purge Amazon Simple Queue Service (SQS) queues, including polling and real-time triggers. Use them to push messages, drain queues into flow outputs, clear a queue or dead-letter queue, or start flows when new messages arrive.",
    categories = { PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.PluginSubGroup;

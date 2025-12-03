@PluginSubGroup(
    title = "SQS",
    description = "Tasks that publish to or consume from Amazon Simple Queue Service (SQS), including polling and real-time triggers. Use them to push messages, drain queues into flow outputs, or start flows when new messages arrive.",
    categories = { PluginSubGroup.PluginCategory.MESSAGING, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.PluginSubGroup;

@PluginSubGroup(
    title = "SQS",
    description = "This sub-group of plugins contains tasks for using Amazon Simple Queue Service (SQS).\n" +
        "Amazon SQS is a fully managed message queuing for microservices, distributed systems, and serverless applications.",
    categories = { PluginSubGroup.PluginCategory.MESSAGING, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.PluginSubGroup;
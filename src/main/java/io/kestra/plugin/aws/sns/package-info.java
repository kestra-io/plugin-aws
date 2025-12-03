@PluginSubGroup(
    title = "SNS",
    description = "Tasks that publish notifications to Amazon Simple Notification Service (SNS) topics for pub/sub fanout or SMS, email, Lambda, and SQS targets, with support for custom message attributes.",
    categories = { PluginSubGroup.PluginCategory.MESSAGING, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.aws.sns;

import io.kestra.core.models.annotations.PluginSubGroup;

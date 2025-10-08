@PluginSubGroup(
    title = "CloudWatch",
    description = "This sub-group of plugins contains tasks for using Amazon CloudWatch.\n" +
        "Amazon CloudWatch is a monitoring and observability service that provides data and actionable insights for AWS resources and applications.",
    categories = { PluginSubGroup.PluginCategory.ALERTING, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.models.annotations.PluginSubGroup;
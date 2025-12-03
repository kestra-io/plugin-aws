@PluginSubGroup(
    title = "CloudWatch",
    description = "Tasks that push custom metrics, query recent CloudWatch metric statistics, and trigger flows when metric queries return data, providing lightweight observability hooks without managing agents.",
    categories = { PluginSubGroup.PluginCategory.ALERTING, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.models.annotations.PluginSubGroup;

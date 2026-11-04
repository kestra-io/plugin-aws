@PluginSubGroup(
    title = "AWS Billing",
    description = """
        Tasks and triggers to query AWS Cost Explorer (cost and usage, forecasts, cost anomalies) and AWS Budgets, and to start flows when a budget threshold is crossed or a new cost anomaly is detected.""",
    categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.BUSINESS }
)
package io.kestra.plugin.aws.billing;

import io.kestra.core.models.annotations.PluginSubGroup;

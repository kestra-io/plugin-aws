@PluginSubGroup(
    description = "Tasks that invoke AWS Lambda functions with optional JSON payloads, capture response bodies or logs, and emit metrics about execution time and payload size for serverless workflows.",
        categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.INFRASTRUCTURE }
)
package io.kestra.plugin.aws.lambda;

import io.kestra.core.models.annotations.PluginSubGroup;

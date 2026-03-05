@PluginSubGroup(
    description = "Tasks that put events on Amazon EventBridge buses with custom detail payloads so routing rules can fan them out to downstream AWS or SaaS targets.",
        categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.DATA }
)
package io.kestra.plugin.aws.eventbridge;

import io.kestra.core.models.annotations.PluginSubGroup;

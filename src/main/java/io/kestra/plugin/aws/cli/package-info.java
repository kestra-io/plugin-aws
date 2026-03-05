@PluginSubGroup(
    title = "CLI",
    description = "Tasks that run AWS CLI commands (inside a container by default), reuse Kestra credentials and environment variables, and capture stdout or files produced by the CLI.",
        categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.DATA, PluginSubGroup.PluginCategory.INFRASTRUCTURE }
)
package io.kestra.plugin.aws.cli;

import io.kestra.core.models.annotations.PluginSubGroup;

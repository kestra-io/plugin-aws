@PluginSubGroup(
    title = "ECR",
    description = "Tasks that fetch an Amazon ECR authorization token for Docker or OCI registry logins, useful before building or pulling images in workflows.",
    categories = { PluginSubGroup.PluginCategory.SCRIPT }
)
package io.kestra.plugin.aws.ecr;

import io.kestra.core.models.annotations.PluginSubGroup;

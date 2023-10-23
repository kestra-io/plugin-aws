@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for get authorization token to Amazon ECR." +
        "This authorization token can be used for the docker CLI to push and pull images with Amazon ECR",
    categories = { PluginSubGroup.PluginCategory.SCRIPT }
)
package io.kestra.plugin.aws.ecr;

import io.kestra.core.models.annotations.PluginSubGroup;

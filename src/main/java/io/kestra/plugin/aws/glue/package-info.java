@PluginSubGroup(
    description = "Tasks that control AWS Glue jobs: start runs with arguments, monitor their status, stop executions, and fetch job metadata to orchestrate serverless Spark ETL pipelines.",
        categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.DATA }
)
package io.kestra.plugin.aws.glue;

import io.kestra.core.models.annotations.PluginSubGroup;

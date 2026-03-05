@PluginSubGroup(
    description = "Tasks that run Amazon Athena SQL queries against data in S3 or federated sources, writing results to an S3 output location and optionally fetching rows. Queries can wait for completion or be submitted asynchronously and emit execution metrics such as data scanned to help monitor cost.",
        categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.DATA }
)
package io.kestra.plugin.aws.athena;

import io.kestra.core.models.annotations.PluginSubGroup;

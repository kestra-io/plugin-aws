@PluginSubGroup(
    description = "Tasks that read and write Amazon DynamoDB tables: put, get, or delete individual items and run query or scan operations with expression attributes. Supports limiting and filtering results, then fetching rows directly or storing them to files for downstream steps.",
    categories = { PluginSubGroup.PluginCategory.DATABASE, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.models.annotations.PluginSubGroup;

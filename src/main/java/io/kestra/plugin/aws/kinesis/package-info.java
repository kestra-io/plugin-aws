@PluginSubGroup(
    description = "Tasks that write batches of records to Amazon Kinesis Data Streams with partition keys for high-throughput, real-time ingestion pipelines.",
        categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.DATA }
)
package io.kestra.plugin.aws.kinesis;

import io.kestra.core.models.annotations.PluginSubGroup;

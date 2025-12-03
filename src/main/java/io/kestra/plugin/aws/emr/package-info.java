@PluginSubGroup(
    title = "EMR",
    description = "Tasks that orchestrate Amazon EMR and EMR Serverless workloads: create or delete clusters, submit steps, start job runs, and optionally wait for completion so Spark or Hadoop pipelines can be managed from Kestra.",
    categories = {PluginSubGroup.PluginCategory.TOOL, PluginSubGroup.PluginCategory.CLOUD}
)
package io.kestra.plugin.aws.emr;

import io.kestra.core.models.annotations.PluginSubGroup;

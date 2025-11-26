@PluginSubGroup(
    title = "EMR",
    description = "This sub-group of plugins contains tasks for using Amazon EMR and EMR Serverless.\n" +
        "Amazon EMR (Elastic MapReduce) is a managed big data platform for running frameworks such as Apache Hadoop and Apache Spark on AWS.\n" +
        "EMR Serverless provides a serverless option that lets you run Spark or Hive jobs without managing clusters.",
    categories = {PluginSubGroup.PluginCategory.TOOL, PluginSubGroup.PluginCategory.CLOUD}
)
package io.kestra.plugin.aws.emr;

import io.kestra.core.models.annotations.PluginSubGroup;
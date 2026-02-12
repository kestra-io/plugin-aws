@PluginSubGroup(
    description = "Tasks that manage Amazon Simple Storage Service (S3) buckets and objects: upload or download single and multiple files, list, copy, and delete keys, create buckets, and trigger flows from bucket events. Supports streaming files from URIs, writing fetched objects to outputs, and handling large transfers via the S3 Transfer Manager.",
        categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.DATA }
)
package io.kestra.plugin.aws.s3;

import io.kestra.core.models.annotations.PluginSubGroup;

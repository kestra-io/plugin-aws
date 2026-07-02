package io.kestra.plugin.aws.msk;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnection;
import io.kestra.plugin.aws.ConnectionUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.ListClustersRequest;
import software.amazon.awssdk.services.kafka.model.ListClustersResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List Amazon MSK clusters",
    description = """
        Returns MSK clusters in the configured AWS region with optional name filtering.
        Paginates automatically. Results are capped at 100 entries; for larger accounts
        use the AWS Console or CLI for full enumeration.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "List all MSK clusters and log the count.",
            full = true,
            code = """
                id: msk_list_clusters
                namespace: company.team

                tasks:
                  - id: list_clusters
                    type: io.kestra.plugin.aws.msk.ListClusters
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"

                  - id: log_results
                    type: io.kestra.plugin.core.log.Log
                    message: "Found {{ outputs.list_clusters.total }} MSK clusters"
                """
        )
    }
)
public class ListClusters extends AbstractConnection implements RunnableTask<ListClusters.Output> {

    private static final int MAX_RESULTS = 100;

    @Schema(
        title = "Cluster name filter",
        description = "Optional substring filter on cluster name."
    )
    @PluginProperty(group = "processing")
    private Property<String> clusterNameFilter;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        // Render once before the loop — avoids redundant Pebble evaluation on every page
        var resolvedNameFilter = clusterNameFilter != null
            ? runContext.render(clusterNameFilter).as(String.class).orElse(null)
            : null;

        var clusters = new ArrayList<Map<String, Object>>();
        String nextToken = null;

        logger.debug("Listing MSK clusters");

        try (var client = client(runContext)) {
            do {
                var remaining = MAX_RESULTS - clusters.size();
                var reqBuilder = ListClustersRequest.builder()
                    .maxResults(Math.min(100, remaining));
                if (resolvedNameFilter != null) {
                    reqBuilder.clusterNameFilter(resolvedNameFilter);
                }
                if (nextToken != null) {
                    reqBuilder.nextToken(nextToken);
                }
                ListClustersResponse response = client.listClusters(reqBuilder.build());
                for (var info : response.clusterInfoList()) {
                    clusters.add(Map.of(
                        "clusterArn", info.clusterArn() != null ? info.clusterArn() : "",
                        "clusterName", info.clusterName() != null ? info.clusterName() : "",
                        "state", info.state() != null ? info.state().toString() : "",
                        "kafkaVersion", info.currentBrokerSoftwareInfo() != null && info.currentBrokerSoftwareInfo().kafkaVersion() != null
                            ? info.currentBrokerSoftwareInfo().kafkaVersion() : "",
                        "numberOfBrokerNodes", info.numberOfBrokerNodes() != null ? info.numberOfBrokerNodes() : 0,
                        "creationTime", info.creationTime() != null ? info.creationTime().toString() : ""
                    ));
                    if (clusters.size() >= MAX_RESULTS) {
                        logger.warn("Result cap of {} reached; further clusters are omitted.", MAX_RESULTS);
                        nextToken = null;
                        break;
                    }
                }
                nextToken = clusters.size() < MAX_RESULTS ? response.nextToken() : null;
            } while (nextToken != null);
        }

        logger.debug("Found {} MSK clusters", clusters.size());
        return Output.builder()
            .clusters(clusters)
            .total(clusters.size())
            .build();
    }

    @VisibleForTesting
    KafkaClient client(RunContext runContext) throws Exception {
        var clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, KafkaClient.builder()).build();
    }

    @SuperBuilder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Clusters",
            description = "List of MSK cluster entries (capped at 100), each containing `clusterArn`, `clusterName`, `state`, `kafkaVersion`, `numberOfBrokerNodes`, and `creationTime`."
        )
        private final List<Map<String, Object>> clusters;

        @Schema(title = "Total", description = "Number of clusters returned (max 100).")
        private final Integer total;
    }
}

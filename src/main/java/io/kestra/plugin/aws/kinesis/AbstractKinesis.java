package io.kestra.plugin.aws.kinesis;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.kestra.plugin.aws.shared.AbstractConnection;
import io.kestra.plugin.aws.shared.ConnectionUtils;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Shared Kinesis connection",
    description = "Provides Kinesis sync/async clients using standard AWS connection settings."
)
public abstract class AbstractKinesis extends AbstractConnection {
    protected KinesisAsyncClient asyncClient(final RunContext runContext) throws Exception {
        final AwsClientConfig config = awsClientConfig(runContext);
        return ConnectionUtils.configureAsyncClient(config, KinesisAsyncClient.builder()).build();
    }

    protected KinesisClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
        final AwsClientConfig clientConfig = awsClientConfig(runContext);
        return ConnectionUtils.configureSyncClient(clientConfig, KinesisClient.builder()).build();
    }

    // A follow-up page request must carry only the token: Kinesis rejects `NextToken` together with `StreamName`.
    static List<Shard> listShards(String stream, Rethrow.FunctionChecked<ListShardsRequest, ListShardsResponse, Exception> call) throws Exception {
        var shards = new ArrayList<Shard>();
        var page = call.apply(ListShardsRequest.builder().streamName(stream).build());
        shards.addAll(page.shards());
        while (page.nextToken() != null) {
            page = call.apply(ListShardsRequest.builder().nextToken(page.nextToken()).build());
            shards.addAll(page.shards());
        }
        return shards;
    }

    public enum IteratorType {
        AT_SEQUENCE_NUMBER,
        AFTER_SEQUENCE_NUMBER,
        TRIM_HORIZON,
        LATEST,
        AT_TIMESTAMP;
    }
}

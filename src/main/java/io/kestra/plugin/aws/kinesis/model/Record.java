package io.kestra.plugin.aws.kinesis.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import jakarta.validation.constraints.NotNull;

@Getter
@Builder
@EqualsAndHashCode
@Jacksonized
public class Record {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();

    @Schema(
        title = "Partition key",
        description = "Determines the target shard for the record."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @JsonAlias("PartitionKey")
    private String partitionKey;

    @Schema(
        title = "Explicit hash key",
        description = "Optional hash value that overrides the partition key hash for shard selection."
    )
    @PluginProperty(dynamic = true)
    @JsonAlias("ExplicitHashKey")
    private String explicitHashKey;

    @Schema(
        title = "Data",
        description = "Data payload as a string; UTF-8 encoded before sending."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @JsonAlias("Data")
    private String data;

    public PutRecordsRequestEntry toPutRecordsRequestEntry(RunContext runContext) throws IllegalVariableEvaluationException {
        var partitionKey = runContext.render(this.partitionKey);
        var explicitHashKey = runContext.render(this.explicitHashKey);
        var data = runContext.render(this.data);
        PutRecordsRequestEntry.Builder builder = PutRecordsRequestEntry.builder()
            .data(SdkBytes.fromUtf8String(data))
            .partitionKey(partitionKey);

        if (!Strings.isNullOrEmpty(explicitHashKey)) {
            builder.explicitHashKey(explicitHashKey);
        }

        return builder
            .build();
    }
}

package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get an item from a table."
)
@Plugin(
    examples = {
        @Example(
            title = "Get an item from its key.",
            code = {
                "tableName: \"persons\"",
                "keyName: \"id\"",
                "keyValue; \"1\""
            }
        )
    }
)
public class GetItem extends AbstractDynamoDb implements RunnableTask<GetItem.Output> {

    @Schema(
        title = "The DynamoDB item key name."
    )
    @PluginProperty
    private String keyName;

    @Schema(
        title = "The DynamoDB key value."
    )
    @PluginProperty(dynamic = true)
    private String keyValue;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (var dynamoDb = client(runContext)) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put(keyName, AttributeValue.fromS(runContext.render(keyValue)));

            var getRequest = GetItemRequest.builder()
                .tableName(runContext.render(this.getTableName()))
                .key(key)
                .build();

            var response = dynamoDb.getItem(getRequest);
            var row = DynamoDbUtils.objectMapFrom(response.item());
            return Output.builder().row(row).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Map containing the fetched item."
        )
        private Map<String, Object> row;
    }
}

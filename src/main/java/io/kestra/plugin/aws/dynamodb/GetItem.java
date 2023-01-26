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
                "key: ",
                "   id: \"1\""
            }
        )
    }
)
public class GetItem extends AbstractDynamoDb implements RunnableTask<GetItem.Output> {
    @Schema(
        title = "The DynamoDB item key.",
        description = "The DynamoDB item key. It's a map of string -> object."
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> key;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (var dynamoDb = client(runContext)) {
            Map<String, AttributeValue> key = valueMapFrom(runContext.render(this.key));

            var getRequest = GetItemRequest.builder()
                .tableName(runContext.render(this.tableName))
                .key(key)
                .build();

            var response = dynamoDb.getItem(getRequest);
            var row = objectMapFrom(response.item());
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

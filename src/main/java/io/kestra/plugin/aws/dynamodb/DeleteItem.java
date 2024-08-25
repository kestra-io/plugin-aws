package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete an item from a table."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete an item by its key.",
            full = true,
            code = """
                id: aws_dynamodb_delete_item
                namespace: company.name

                tasks:
                  - id: delete_item
                    type: io.kestra.plugin.aws.dynamodb.DeleteItem
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    tableName: "persons"
                    key: 
                       id: "1"
                """
        )
    }
)
public class DeleteItem extends AbstractDynamoDb implements RunnableTask<VoidOutput> {
    @Schema(
        title = "The DynamoDB item key.",
        description = "The DynamoDB item identifier."
    )
    @PluginProperty
    private Map<String, Object> key;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        try (var dynamoDb = client(runContext)) {
            Map<String, AttributeValue> key = valueMapFrom(getKey());

            var deleteRequest = DeleteItemRequest.builder()
                .tableName(runContext.render(this.getTableName()))
                .key(key)
                .build();

            dynamoDb.deleteItem(deleteRequest);
            return null;
        }
    }
}

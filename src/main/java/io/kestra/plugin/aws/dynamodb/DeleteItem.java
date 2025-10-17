package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
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
    title = "Delete an item from a DynamoDB table."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete an item by its key.",
            full = true,
            code = """
                id: aws_dynamodb_delete_item
                namespace: company.team

                tasks:
                  - id: delete_item
                    type: io.kestra.plugin.aws.dynamodb.DeleteItem
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
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
        title = "The DynamoDB item key",
        description = "The DynamoDB item identifier"
    )
    private Property<Map<String, Object>> key;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        try (var dynamoDb = client(runContext)) {
            var renderedKey = runContext.render(this.key).asMap(String.class, Object.class);
            Map<String, AttributeValue> key = valueMapFrom(renderedKey);

            var deleteRequest = DeleteItemRequest.builder()
                .tableName(runContext.render(this.getTableName()).as(String.class).orElseThrow())
                .key(key)
                .build();

            dynamoDb.deleteItem(deleteRequest);
            return null;
        }
    }
}

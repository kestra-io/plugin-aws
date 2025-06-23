package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

import java.util.Map;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query items from a DynamoDB table."
)
@Plugin(
    examples = {
        @Example(
            title = "Query items from a table.",
            full = true,
            code = """
                id: aws_dynamo_db_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.aws.dynamodb.Query
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    tableName: "persons"
                    keyConditionExpression: id = :id
                    expressionAttributeValues:
                      :id: "1"
                """
        ),
        @Example(
            title = "Query items from a table with a filter expression.",
            full = true,
            code = """
                id: aws_dynamo_db_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.aws.dynamodb.Query
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    tableName: "persons"
                    keyConditionExpression: id = :id
                    expressionAttributeValues:
                      :id: "1"
                      :lastname: "Doe"
                """
        )
    }
)
public class Query extends AbstractDynamoDb implements RunnableTask<FetchOutput> {
    @Schema(
        title = "The way you want to store the data.",
        description = "FETCH_ONE output the first row, "
            + "FETCH output all the rows, "
            + "STORE store all rows in a file, "
            + "NONE do nothing."
    )
    @Builder.Default
    private Property<FetchType> fetchType = Property.of(FetchType.STORE);

    @Schema(
        title = "Maximum numbers of returned results."
    )
    private Property<Integer> limit;

    @Schema(
        title = "Query key condition expression."
    )
    @NotNull
    private Property<String> keyConditionExpression;

    @Schema(
        title = "Query expression attributes.",
        description = "It's a map of string -> object."
    )
    @NotNull
    private Property<Map<String, Object>> expressionAttributeValues;

    @Schema(
        title = "Query filter expression.",
        description = "Query filter expression."
    )
    private Property<String> filterExpression;

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        try (var dynamoDb = client(runContext)) {
            var queryBuilder = QueryRequest.builder()
                .tableName(runContext.render(this.getTableName()).as(String.class).orElseThrow())
                .keyConditionExpression(runContext.render(keyConditionExpression).as(String.class).orElseThrow())
                .expressionAttributeValues(valueMapFrom(runContext.render(expressionAttributeValues).asMap(String.class, Object.class)));

            if(limit != null) {
                queryBuilder.limit(runContext.render(limit).as(Integer.class).orElseThrow());
            }
            if(filterExpression != null){
                queryBuilder.filterExpression(runContext.render(filterExpression).as(String.class).orElseThrow());
            }

            var query = queryBuilder.build();
            var items = dynamoDb.query(query).items();

            return this.fetchOutputs(items, runContext.render(this.fetchType).as(FetchType.class).orElseThrow(), runContext);
        }
    }
}

package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "Query items from a table."
)
@Plugin(
    examples = {
        @Example(
            title = "Query items from a table.",
            full = true,
            code = """
                id: aws_dynamo_db_query
                namespace: company.name

                tasks:
                  - id: query
                    type: io.kestra.plugin.aws.dynamodb.Query
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
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
                namespace: company.name

                tasks:
                  - id: query
                    type: io.kestra.plugin.aws.dynamodb.Query
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
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
    @PluginProperty
    private FetchType fetchType = FetchType.STORE;

    @Schema(
        title = "Maximum numbers of returned results."
    )
    @PluginProperty
    private Integer limit;

    @Schema(
        title = "Query key condition expression."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String keyConditionExpression;

    @Schema(
        title = "Query expression attributes.",
        description = "It's a map of string -> object."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Map<String, Object> expressionAttributeValues;

    @Schema(
        title = "Query filter expression.",
        description = "Query filter expression."
    )
    @PluginProperty(dynamic = true)
    private String filterExpression;

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        try (var dynamoDb = client(runContext)) {
            var queryBuilder = QueryRequest.builder()
                .tableName(runContext.render(this.getTableName()))
                .keyConditionExpression(runContext.render(keyConditionExpression))
                .expressionAttributeValues(valueMapFrom(expressionAttributeValues));

            if(limit != null) {
                queryBuilder.limit(limit);
            }
            if(filterExpression != null){
                queryBuilder.filterExpression(runContext.render(filterExpression));
            }

            var query = queryBuilder.build();
            var items = dynamoDb.query(query).items();

            return this.fetchOutputs(items, this.fetchType, runContext);
        }
    }
}

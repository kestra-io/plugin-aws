package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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
    title = "Query DynamoDB items by key condition",
    description = "Executes a Query request with the provided keyConditionExpression. fetchType defaults to STORE, writing results to internal storage; FETCH and FETCH_ONE load results into memory. Optional limit and filterExpression refine results."
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
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "items",
            description = "Number of items fetched from DynamoDB"
        )
    }
)
public class Query extends AbstractDynamoDb implements RunnableTask<FetchOutput> {
    @Schema(
        title = "Fetch strategy",
        description = "STORE (default) writes rows to internal storage; FETCH loads all rows; FETCH_ONE returns the first row."
    )
    @Builder.Default
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Schema(
        title = "Max results",
        description = "Maximum items to return."
    )
    private Property<Integer> limit;

    @Schema(
        title = "Key condition expression",
        description = "Expression using partition key (and sort key if defined)."
    )
    @NotNull
    private Property<String> keyConditionExpression;

    @Schema(
        title = "Expression attribute values",
        description = "Map of placeholders used in expressions."
    )
    @NotNull
    private Property<Map<String, Object>> expressionAttributeValues;

    @Schema(
        title = "Filter expression",
        description = "Additional server-side filter; requires expressionAttributeValues."
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

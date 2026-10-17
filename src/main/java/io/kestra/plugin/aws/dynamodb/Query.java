package io.kestra.plugin.aws.dynamodb;

import java.util.Map;

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
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query DynamoDB items by key condition",
    description = "Executes a Query and follows LastEvaluatedKey until every matching item is read. fetchType defaults to STORE, writing results to internal storage; FETCH and FETCH_ONE load results into memory. Optional limit stops once that many matching items are collected."
)
@Plugin(
    examples = {
        @Example(
            title = "Query items from a table",
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
            title = "Query items from a table with a filter expression",
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
                    filterExpression: "lastname = :lastname"
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
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Schema(
        title = "Max results",
        description = "Maximum number of matching items to return across pages. Minimum 1 when set. Leave empty to return every match. This is not a DynamoDB page size."
    )
    @PluginProperty(group = "processing")
    private Property<@Min(1) Integer> limit;

    @Schema(
        title = "Key condition expression",
        description = "Expression using partition key (and sort key if defined)."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> keyConditionExpression;

    @Schema(
        title = "Expression attribute values",
        description = "Map of placeholders used in expressions."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> expressionAttributeValues;

    @Schema(
        title = "Filter expression",
        description = "Additional server-side filter; requires expressionAttributeValues."
    )
    @PluginProperty(group = "processing")
    private Property<String> filterExpression;

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        stopIfKilled();
        try (var dynamoDb = client(runContext)) {
            onKill(runContext, dynamoDb::close);
            var rTable = runContext.render(this.tableName).as(String.class).orElseThrow(() -> missingProperty("tableName"));
            var rKeyCondition = runContext.render(keyConditionExpression).as(String.class).orElseThrow(() -> missingProperty("keyConditionExpression"));
            var rValues = valueMapFrom(runContext.render(expressionAttributeValues).asMap(String.class, Object.class));
            var rFetch = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.STORE);
            var rMaxResults = maxResults(runContext, this.limit, rFetch);
            String rFilter = filterExpression == null
                ? null
                : runContext.render(filterExpression).as(String.class).orElseThrow(() -> missingProperty("filterExpression"));

            return this.fetchOutputs(rFetch, rMaxResults, runContext, (exclusiveStartKey, pageLimit) ->
            {
                var queryBuilder = QueryRequest.builder()
                    .tableName(rTable)
                    .keyConditionExpression(rKeyCondition)
                    .expressionAttributeValues(rValues);
                if (exclusiveStartKey != null) {
                    queryBuilder.exclusiveStartKey(exclusiveStartKey);
                }
                // DynamoDB Limit caps items evaluated, not items matching the filter.
                if (pageLimit != null && rFilter == null) {
                    queryBuilder.limit(pageLimit);
                }
                if (rFilter != null) {
                    queryBuilder.filterExpression(rFilter);
                }
                var response = dynamoDb.query(queryBuilder.build());
                var nextKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
                return new DynamoPage(response.items(), nextKey);
            });
        }
    }
}

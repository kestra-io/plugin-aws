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
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Scan items from a DynamoDB table",
    description = "Performs a Scan over the table and follows LastEvaluatedKey until every matching item is read. fetchType defaults to STORE; FETCH and FETCH_ONE read into memory. Optional limit stops once that many matching items are collected."
)
@Plugin(
    examples = {
        @Example(
            title = "Scan all items from a table",
            full = true,
            code = """
                id: aws_dynamo_db_scan
                namespace: company.team

                tasks:
                  - id: scan
                    type: io.kestra.plugin.aws.dynamodb.Scan
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    tableName: "persons"
                """
        ),
        @Example(
            title = "Scan items from a table with a filter expression",
            full = true,
            code = """
                id: aws_dynamo_db_scan
                namespace: company.team

                tasks:
                  - id: scan
                    type: io.kestra.plugin.aws.dynamodb.Scan
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    tableName: "persons"
                    filterExpression: "lastname = :lastname"
                    expressionAttributeValues:
                      :lastname: "Doe"
                """
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "items",
            description = "Number of items scanned from DynamoDB"
        )
    }
)
public class Scan extends AbstractDynamoDb implements RunnableTask<FetchOutput> {

    @Schema(
        title = "Fetch strategy",
        description = "STORE (default) writes rows to internal storage; FETCH loads all rows; FETCH_ONE returns the first row."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Min(1)
    @Schema(
        title = "Max results",
        description = "Maximum number of matching items to return across pages. Minimum 1 when set. Leave empty to return every match. This is not a DynamoDB page size."
    )
    @PluginProperty(group = "processing")
    private Property<Integer> limit;

    @Schema(
        title = "Filter expression",
        description = "Server-side filter applied after the scan; requires expressionAttributeValues."
    )
    @PluginProperty(group = "processing")
    private Property<String> filterExpression;

    @Schema(
        title = "Expression attribute values",
        description = "Map of placeholders used in expressions."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> expressionAttributeValues;

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        stopIfKilled();
        try (var dynamoDb = client(runContext)) {
            onKill(runContext, dynamoDb::close);
            var rTable = runContext.render(this.tableName).as(String.class).orElseThrow(() -> missingProperty("tableName"));
            var rFetch = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.STORE);
            var rMaxResults = maxResults(runContext, this.limit, rFetch);

            String rFilter;
            Map<String, AttributeValue> rValues;
            if (filterExpression != null) {
                var attributes = runContext.render(expressionAttributeValues).asMap(String.class, Object.class);
                if (attributes.isEmpty()) {
                    throw new IllegalArgumentException("'expressionAttributeValues' must be provided when 'filterExpression' is used");
                }
                rFilter = runContext.render(filterExpression).as(String.class).orElseThrow(() -> missingProperty("filterExpression"));
                rValues = valueMapFrom(attributes);
            } else {
                rFilter = null;
                rValues = null;
            }

            var items = readPages(rMaxResults, (exclusiveStartKey, pageLimit) ->
            {
                var scanBuilder = ScanRequest.builder().tableName(rTable);
                if (exclusiveStartKey != null) {
                    scanBuilder.exclusiveStartKey(exclusiveStartKey);
                }
                if (pageLimit != null) {
                    scanBuilder.limit(pageLimit);
                }
                if (rFilter != null) {
                    scanBuilder.filterExpression(rFilter);
                    scanBuilder.expressionAttributeValues(rValues);
                }
                var response = dynamoDb.scan(scanBuilder.build());
                var nextKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
                return new DynamoPage(response.items(), nextKey);
            });

            return this.fetchOutputs(items, rFetch, runContext);
        }
    }
}

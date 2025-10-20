package io.kestra.plugin.aws.dynamodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Scan items from a DynamoDB table."
)
@Plugin(
    examples = {
        @Example(
            title = "Scan all items from a table.",
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
            title = "Scan items from a table with a filter expression.",
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
public class Scan  extends AbstractDynamoDb implements RunnableTask<FetchOutput> {

    @Schema(
        title = "The way you want to store the data.",
        description = "FETCH_ONE output the first row, "
            + "FETCH output all the rows, "
            + "STORE store all rows in a file, "
            + "NONE do nothing."
    )
    @Builder.Default
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Schema(
        title = "Maximum numbers of returned results"
    )
    private Property<Integer> limit;

    @Schema(
        title = "Scan filter expression",
        description = "When used, `expressionAttributeValues` property must also be provided."
    )
    private Property<String> filterExpression;

    @Schema(
        title = "Scan expression attributes",
        description = "It's a map of string -> object."
    )
    private Property<Map<String, Object>> expressionAttributeValues;


    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        try (var dynamoDb = client(runContext)) {
            var scanBuilder = ScanRequest.builder()
                .tableName(runContext.render(this.getTableName()).as(String.class).orElseThrow());

            if(limit != null) {
                scanBuilder.limit(runContext.render(limit).as(Integer.class).orElseThrow());
            }
            if(filterExpression != null){
                var attributes = runContext.render(expressionAttributeValues).asMap(String.class, Object.class);
                if(attributes.isEmpty()){
                    throw new IllegalArgumentException("'expressionAttributeValues' must be provided when 'filterExpression' is used");
                }
                scanBuilder.filterExpression(runContext.render(filterExpression).as(String.class).orElseThrow());
                scanBuilder.expressionAttributeValues(valueMapFrom(attributes));
            }


            var scan = scanBuilder.build();
            var items = dynamoDb.scan(scan).items();

            return this.fetchOutputs(items, runContext.render(this.fetchType).as(FetchType.class).orElseThrow(), runContext);
        }
    }
}

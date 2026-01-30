package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.models.annotations.*;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push custom metrics to CloudWatch",
    description = "Writes one or more MetricDatum entries into a CloudWatch namespace. Uses current time for timestamps and optionally sets unit and dimensions."
)
@Plugin(
    examples =
        @Example(
            full = true,
            title = "Push custom metrics to CloudWatch",
            code = """
                id: aws_cloudwatch_push
                namespace: company.team

                tasks:
                  - id: push_metric
                    type: io.kestra.plugin.aws.cloudwatch.Push
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "us-east-1"
                    namespace: "Custom/MyApp"
                    metrics:
                      - metricName: "RequestsCount"
                        value: 42.0
                        unit: "Count"
                        dimensions:
                          env: "prod"
                          service: "payments"
                      - metricName: "LatencyMs"
                        value: 123.4
                        unit: "Milliseconds"
                        dimensions:
                          env: "prod"
                          service: "payments"
                """
    )
)
public class Push extends AbstractCloudWatch implements RunnableTask<Push.Output> {
    @NotNull
    @Schema(
        title = "CloudWatch namespace",
        description = "Target namespace for metric data, e.g., Custom/MyApp."
    )
    private Property<String> namespace;

    @Schema(
        title = "Metrics",
        description = "List of metrics to publish; each becomes a MetricDatum."
    )
    @NotNull
    private Property<List<MetricValue>> metrics;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rNamespace = runContext.render(this.namespace).as(String.class).orElseThrow();
        List<MetricValue> rMetrics = runContext.render(this.metrics).asList(MetricValue.class);

        try (CloudWatchClient client = this.client(runContext)) {
            List<MetricDatum> data = rMetrics.stream()
                .map(throwFunction(mv -> {
                    var rMetricName = runContext.render(mv.getMetricName()).as(String.class).orElseThrow();
                    var rValue = runContext.render(mv.getValue()).as(Double.class).orElseThrow();

                    MetricDatum.Builder datum = MetricDatum.builder()
                        .metricName(rMetricName)
                        .timestamp(Instant.now())
                        .value(rValue);

                    runContext.render(mv.getUnit()).as(String.class).ifPresent(rUnit -> datum.unit(StandardUnit.fromValue(rUnit)));

                    Map<String, Object> rDims = runContext.render(mv.getDimensions()).asMap(String.class, Object.class);
                    if (!rDims.isEmpty()) {
                        datum.dimensions(
                            rDims.entrySet().stream()
                                .map(e -> Dimension.builder()
                                    .name(e.getKey())
                                    .value(String.valueOf(e.getValue()))
                                    .build())
                                .toList()
                        );
                    }

                    return datum.build();
                }))
                .toList();

            client.putMetricData(PutMetricDataRequest.builder()
                .namespace(rNamespace)
                .metricData(data)
                .build());

            runContext.logger().info("Pushed {} datapoints to CloudWatch namespace {}", data.size(), rNamespace);

            return Output.builder()
                .count(data.size())
                .build();
        }
    }

    @Builder
    @Getter
    public static class MetricValue {
        @Schema(
            title = "Metric name",
            description = "Name of the datapoint."
        )
        @NotNull
        private Property<String> metricName;

        @Schema(
            title = "Metric value",
            description = "Numeric value sent for the metric."
        )
        private Property<Double> value;

        @Schema(
            title = "Metric unit",
            description = "Optional CloudWatch unit string, e.g., Count or Milliseconds."
        )
        private Property<String> unit;

        @Schema(
            title = "Metric dimensions",
            description = "Optional key/value dimensions attached to the datapoint."
        )
        private Property<Map<String, Object>> dimensions;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Datapoints pushed",
            description = "Total MetricDatum entries sent in the request."
        )
        private final Integer count;
    }
}

package io.kestra.plugin.aws.cloudwatch;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.annotations.*;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Query CloudWatch metrics.")
@Plugin(
    examples =
        @Example(
            title = "Query CPU utilization over the last 5 minutes",
            code = """
                id: aws_cloudwatch_query
                namespace: company.team
                tasks:
                  - id: query
                    type: io.kestra.plugin.aws.cloudwatch.Query
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "us-east-1"
                    namespace: "AWS/EC2"
                    metricName: "CPUUtilization"
                    statistic: "Average"
                    periodSeconds: 60
                    window: PT5M
                    dimensions:
                      - name: "InstanceId"
                        value: "i-0abcd1234ef567890"
                """
    )
)
public class Query extends AbstractCloudWatch implements RunnableTask<Query.Output> {
    @Schema(title = "CloudWatch namespace (optional, required for metric-based queries)")
    private Property<String> namespace;

    @Schema(title = "The metric name to query")
    @NotNull
    private Property<String> metricName;

    @Schema(title = "Dimensions to filter the metric")
    private Property<List<DimensionKV>> dimensions;

    @Schema(title = "Statistic type to retrieve", description = "e.g., Average, Sum, Maximum, Minimum, SampleCount")
    @Builder.Default
    private Property<String> statistic = Property.ofValue("Average");

    @Schema(title = "Period in seconds for data aggregation")
    @Builder.Default
    private Property<Integer> periodSeconds = Property.ofValue(60);

    @Schema(
        title = "Time window to query",
        description = "Duration looking back from now, e.g., PT5M for 5 minutes"
    )
    @Builder.Default
    private Property<Duration> window = Property.ofValue(Duration.ofMinutes(5));

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rNamespace = runContext.render(this.namespace).as(String.class).orElse(null);
        String rMetricName = runContext.render(this.metricName).as(String.class).orElseThrow();
        String rStatistic = runContext.render(this.statistic).as(String.class).orElse("Average");
        int rPeriod = runContext.render(this.periodSeconds).as(Integer.class).orElse(60);
        Duration rWindow = runContext.render(this.window).as(Duration.class).orElse(Duration.ofMinutes(5));
        List<DimensionKV> rDims = runContext.render(this.dimensions).asList(DimensionKV.class);

        Instant end = Instant.now();
        Instant start = end.minusSeconds(rWindow.getSeconds());

        try (CloudWatchClient cw = this.client(runContext)) {
            GetMetricStatisticsRequest.Builder req = GetMetricStatisticsRequest.builder()
                .metricName(rMetricName)
                .period(rPeriod)
                .startTime(start)
                .endTime(end)
                .statisticsWithStrings(rStatistic);

            if (rNamespace != null) {
                req.namespace(rNamespace);
            }

            if (rDims != null && !rDims.isEmpty()) {
                req.dimensions(
                    rDims.stream()
                        .map(throwFunction(d -> Dimension.builder()
                            .name(runContext.render(d.getName()).as(String.class).orElseThrow())
                            .value(runContext.render(d.getValue()).as(String.class).orElseThrow())
                            .build()
                        ))
                        .toList()
                );
            }

            GetMetricStatisticsResponse resp = cw.getMetricStatistics(req.build());

            List<Map<String, Object>> series = resp.datapoints().stream()
                .sorted(Comparator.comparing(Datapoint::timestamp))
                .map(dp -> {
                    Map<String, Object> map = JacksonMapper.ofJson().convertValue(dp, new TypeReference<Map<String, Object>>() {});
                    map.put("timestamp", dp.timestamp().toString());
                    map.put("unit", dp.unitAsString());
                    return map;
                })
                .toList();

            runContext.logger().info("Fetched {} datapoints for {}:{}", series.size(), rNamespace, rMetricName);

            return Output.builder()
                .count(series.size())
                .series(series)
                .build();
        }
    }

    @Builder
    @Getter
    public static class DimensionKV {
        @Schema(title = "Dimension name")
        private Property<String> name;

        @Schema(title = "Dimension value")
        private Property<String> value;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Total number of datapoints returned")
        private final Integer count;

        @Schema(title = "Time series data with timestamps and values")
        private final List<Map<String, Object>> series;
    }
}

package io.kestra.plugin.aws.cloudwatch;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.AbstractFlociTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Testcontainers
class CloudWatchLogsTest extends AbstractFlociTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void createLogGroupAndPutEvents() throws Exception {
        var runContext = runContextFactory.of();
        var logGroupName = "/kestra/test/logs";
        var logStreamName = "test-stream";

        var task = CloudWatchLogs.builder()
            .endpointOverride(Property.ofValue(endpointUrl()))
            .accessKeyId(Property.ofValue(ACCESS_KEY))
            .secretKeyId(Property.ofValue(SECRET_KEY))
            .region(Property.ofValue(REGION))
            .build();

        try (var client = task.logsClient(runContext)) {
            client.createLogGroup(r -> r.logGroupName(logGroupName));
            client.createLogStream(r -> r
                .logGroupName(logGroupName)
                .logStreamName(logStreamName));

            client.putLogEvents(r -> r
                .logGroupName(logGroupName)
                .logStreamName(logStreamName)
                .logEvents(InputLogEvent.builder()
                    .timestamp(System.currentTimeMillis())
                    .message("test log event from Floci integration test")
                    .build()));

            var groups = client.describeLogGroups(r -> r.logGroupNamePrefix("/kestra/test"));
            assertThat(groups.logGroups(), hasSize(greaterThanOrEqualTo(1)));
            assertThat(groups.logGroups().getFirst().logGroupName(), is(logGroupName));
        }
    }
}

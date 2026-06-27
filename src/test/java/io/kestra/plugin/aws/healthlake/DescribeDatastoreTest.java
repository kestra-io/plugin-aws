package io.kestra.plugin.aws.healthlake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.healthlake.HealthLakeClient;
import software.amazon.awssdk.services.healthlake.model.*;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DescribeDatastoreTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenDatastoreId_whenDescribe_thenOutputContainsMetadata() throws Exception {
        var runContext = runContextFactory.of();

        var task = DescribeDatastore.builder()
            .id("test-describe-datastore")
            .type(DescribeDatastore.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .datastoreId(Property.ofValue("ds-abc123"))
            .build();

        var props = DatastoreProperties.builder()
            .datastoreId("ds-abc123")
            .datastoreArn("arn:aws:healthlake:us-east-1:123456789012:datastore/ds-abc123")
            .datastoreName("my-fhir-store")
            .datastoreStatus(DatastoreStatus.ACTIVE)
            .datastoreEndpoint("https://healthlake.us-east-1.amazonaws.com/datastore/ds-abc123/r4/")
            .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
            .build();

        var mockResponse = DescribeFHIRDatastoreResponse.builder()
            .datastoreProperties(props)
            .build();

        var mockClient = mock(HealthLakeClient.class);
        when(mockClient.describeFHIRDatastore(any(DescribeFHIRDatastoreRequest.class))).thenReturn(mockResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getDatastoreId(), is("ds-abc123"));
        assertThat(output.getDatastoreName(), is("my-fhir-store"));
        assertThat(output.getDatastoreStatus(), is("ACTIVE"));
        assertThat(output.getDatastoreEndpoint(), containsString("ds-abc123"));
        assertThat(output.getCreatedAt(), notNullValue());
    }
}

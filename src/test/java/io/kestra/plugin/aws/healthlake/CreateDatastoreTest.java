package io.kestra.plugin.aws.healthlake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.healthlake.HealthLakeClient;
import software.amazon.awssdk.services.healthlake.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class CreateDatastoreTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenValidName_whenCreate_thenOutputContainsId() throws Exception {
        var runContext = runContextFactory.of();

        var task = CreateDatastore.builder()
            .id("test-create-datastore")
            .type(CreateDatastore.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .datastoreName(Property.ofValue("my-fhir-store"))
            .build();

        var mockResponse = CreateFHIRDatastoreResponse.builder()
            .datastoreId("ds-abc123")
            .datastoreArn("arn:aws:healthlake:us-east-1:123456789012:datastore/ds-abc123")
            .datastoreStatus(DatastoreStatus.CREATING)
            .datastoreEndpoint("https://healthlake.us-east-1.amazonaws.com/datastore/ds-abc123/r4/")
            .build();

        var mockClient = mock(HealthLakeClient.class);
        when(mockClient.createFHIRDatastore(any(CreateFHIRDatastoreRequest.class))).thenReturn(mockResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getDatastoreId(), is("ds-abc123"));
        assertThat(output.getDatastoreStatus(), is("CREATING"));
        assertThat(output.getDatastoreArn(), containsString("ds-abc123"));
    }
}

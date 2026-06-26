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
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class ListDatastoresTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void givenNoFilter_whenList_thenReturnsAllStores() throws Exception {
        var runContext = runContextFactory.of();

        var task = ListDatastores.builder()
            .id("test-list-datastores")
            .type(ListDatastores.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .build();

        var store1 = DatastoreProperties.builder()
            .datastoreId("ds-001")
            .datastoreArn("arn:aws:healthlake:us-east-1:123456789012:datastore/ds-001")
            .datastoreName("store-one")
            .datastoreStatus(DatastoreStatus.ACTIVE)
            .datastoreEndpoint("https://healthlake.us-east-1.amazonaws.com/datastore/ds-001/r4/")
            .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
            .build();

        var store2 = DatastoreProperties.builder()
            .datastoreId("ds-002")
            .datastoreArn("arn:aws:healthlake:us-east-1:123456789012:datastore/ds-002")
            .datastoreName("store-two")
            .datastoreStatus(DatastoreStatus.CREATING)
            .datastoreEndpoint("https://healthlake.us-east-1.amazonaws.com/datastore/ds-002/r4/")
            .createdAt(Instant.parse("2024-02-01T00:00:00Z"))
            .build();

        var mockResponse = ListFHIRDatastoresResponse.builder()
            .datastorePropertiesList(List.of(store1, store2))
            .build();

        var mockClient = mock(HealthLakeClient.class);
        when(mockClient.listFHIRDatastores(any(ListFHIRDatastoresRequest.class))).thenReturn(mockResponse);

        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        var output = spy.run(runContext);

        assertThat(output.getTotal(), is(2));
        assertThat(output.getDatastores(), hasSize(2));
        assertThat(output.getDatastores().get(0).get("datastoreId"), is("ds-001"));
        assertThat(output.getDatastores().get(1).get("datastoreStatus"), is("CREATING"));
    }

    @Test
    void givenInvalidStatusFilter_whenList_thenThrowsIllegalArgumentException() throws Exception {
        var runContext = runContextFactory.of();

        var task = ListDatastores.builder()
            .id("test-list-datastores-bad-filter")
            .type(ListDatastores.class.getName())
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .filterStatus(Property.ofValue("INVALID_STATUS"))
            .build();

        var mockClient = mock(HealthLakeClient.class);
        var spy = spy(task);
        doReturn(mockClient).when(spy).client(any(RunContext.class));

        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class, () -> spy.run(runContext));
    }
}

package io.kestra.plugin.aws.s3;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.aws.s3.models.S3Object;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@KestraTest
class S3ServiceListTest {

    @Inject
    protected RunContextFactory runContextFactory;

    // Verifies the V2 isTruncated -> continuationToken -> next page loop.
    @Test
    void list_paginatesAcrossTwoTruncatedPages() throws Exception {
        RunContext runContext = runContextFactory.of();

        var mockClient = twoPageClient(List.of("file1.txt", "file2.txt"), List.of("file3.txt"));

        var task = listTask()
            .maxKeys(Property.ofValue(1000))
            .maxFiles(Property.ofValue(1000))
            .build();

        List<S3Object> result = S3Service.list(runContext, mockClient, task, task);

        assertThat(result, hasSize(3));
        assertThat(result.get(0).getKey(), is("file1.txt"));
        assertThat(result.get(1).getKey(), is("file2.txt"));
        assertThat(result.get(2).getKey(), is("file3.txt"));

        // Verify the second call carried the continuation token.
        verify(mockClient, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void shouldContinuePaginationWhenInitialPageDoesNotMatchFilter() throws Exception {
        var mockClient = twoPageClient(List.of("archive/old1.txt", "archive/old2.txt"), List.of("data/orders.csv"));

        var task = listTask()
            .regexp(Property.ofValue(".*\\.csv"))
            .maxKeys(Property.ofValue(2))
            .build();

        var result = S3Service.list(runContextFactory.of(), mockClient, task, task);

        assertThat(result, hasSize(1));
        assertThat(result.getFirst().getKey(), is("data/orders.csv"));
        // a regexp can skip most keys, so pages are requested at the S3 maximum instead of maxKeys
        verify(mockClient, times(2)).listObjectsV2(argThat((ListObjectsV2Request req) -> req.maxKeys() == 1000));
    }

    @Test
    void shouldStopPaginationWhenMatchingCountReachesMaxKeys() throws Exception {
        var mockClient = twoPageClient(List.of("orders-1.csv", "notes.txt"), List.of("orders-2.csv", "orders-3.csv"));

        var task = listTask()
            .regexp(Property.ofValue(".*\\.csv"))
            .maxKeys(Property.ofValue(2))
            .build();

        var result = S3Service.list(runContextFactory.of(), mockClient, task, task);

        assertThat(result.stream().map(S3Object::getKey).toList(), contains("orders-1.csv", "orders-2.csv"));
        verify(mockClient, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void shouldContinuePaginationWhenInitialPageHasOnlyDirectories() throws Exception {
        var mockClient = twoPageClient(List.of("archive/", "data/"), List.of("data/orders.csv"));

        var task = listTask()
            .filter(Property.ofValue(ListInterface.Filter.FILES))
            .maxKeys(Property.ofValue(1))
            .build();

        var result = S3Service.list(runContextFactory.of(), mockClient, task, task);

        assertThat(result, hasSize(1));
        assertThat(result.getFirst().getKey(), is("data/orders.csv"));
        verify(mockClient, times(2)).listObjectsV2(argThat((ListObjectsV2Request req) -> req.maxKeys() == 1000));
    }

    @Test
    void shouldRequestMaxKeysSizedPagesWithoutFilter() throws Exception {
        var mockClient = mock(S3Client.class);
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(ListObjectsV2Response.builder().isTruncated(false).build());
        var task = listTask()
            .maxKeys(Property.ofValue(5))
            .build();

        S3Service.list(runContextFactory.of(), mockClient, task, task);

        verify(mockClient).listObjectsV2(argThat((ListObjectsV2Request req) -> req.maxKeys() == 5));
    }

    @Test
    void shouldReturnNothingWithoutCallingS3WhenMaxKeysIsZero() throws Exception {
        var mockClient = mock(S3Client.class);
        var task = listTask()
            .maxKeys(Property.ofValue(0))
            .build();

        var result = S3Service.list(runContextFactory.of(), mockClient, task, task);

        assertThat(result, empty());
        verify(mockClient, never()).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void shouldRejectNegativeMaxKeys() {
        var mockClient = mock(S3Client.class);
        var task = listTask()
            .maxKeys(Property.ofValue(-1))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> S3Service.list(runContextFactory.of(), mockClient, task, task));

        assertThat(exception.getMessage(), containsString("must be 0 or more"));
        verify(mockClient, never()).listObjectsV2(any(ListObjectsV2Request.class));
    }

    private static io.kestra.plugin.aws.s3.List.ListBuilder<?, ?> listTask() {
        return io.kestra.plugin.aws.s3.List.builder()
            .id("s3-service-list-%s".formatted(IdUtils.create()))
            .type(io.kestra.plugin.aws.s3.List.class.getName())
            .bucket(Property.ofValue("my-bucket"));
    }

    // Serves firstPage, then secondPage once the request carries the continuation token.
    private static S3Client twoPageClient(List<String> firstPage, List<String> secondPage) {
        var page1 = ListObjectsV2Response.builder()
            .isTruncated(true)
            .nextContinuationToken("token-page2")
            .contents(s3Objects(firstPage))
            .build();

        var page2 = ListObjectsV2Response.builder()
            .isTruncated(false)
            .contents(s3Objects(secondPage))
            .build();

        var mockClient = mock(S3Client.class);
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenAnswer(inv -> "token-page2".equals(inv.<ListObjectsV2Request>getArgument(0).continuationToken()) ? page2 : page1);
        return mockClient;
    }

    private static List<software.amazon.awssdk.services.s3.model.S3Object> s3Objects(List<String> keys) {
        return keys.stream()
            .map(key -> software.amazon.awssdk.services.s3.model.S3Object.builder().key(key).size(10L).build())
            .toList();
    }
}

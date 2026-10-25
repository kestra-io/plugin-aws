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

        var obj1 = software.amazon.awssdk.services.s3.model.S3Object.builder()
            .key("file1.txt").size(10L).build();
        var obj2 = software.amazon.awssdk.services.s3.model.S3Object.builder()
            .key("file2.txt").size(20L).build();
        var obj3 = software.amazon.awssdk.services.s3.model.S3Object.builder()
            .key("file3.txt").size(30L).build();

        var page1 = ListObjectsV2Response.builder()
            .isTruncated(true)
            .nextContinuationToken("token-page2")
            .contents(obj1, obj2)
            .build();

        var page2 = ListObjectsV2Response.builder()
            .isTruncated(false)
            .contents(obj3)
            .build();

        S3Client mockClient = mock(S3Client.class);
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenAnswer(inv ->
            {
                ListObjectsV2Request req = inv.getArgument(0);
                return "token-page2".equals(req.continuationToken()) ? page2 : page1;
            });

        var task = io.kestra.plugin.aws.s3.List.builder()
            .id("s3-service-list-unit-test")
            .type(io.kestra.plugin.aws.s3.List.class.getName())
            .bucket(Property.ofValue("my-bucket"))
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
        var runContext = runContextFactory.of();

        var obj1 = software.amazon.awssdk.services.s3.model.S3Object.builder()
            .key("archive/old1.txt").size(10L).build();
        var obj2 = software.amazon.awssdk.services.s3.model.S3Object.builder()
            .key("archive/old2.txt").size(20L).build();
        var obj3 = software.amazon.awssdk.services.s3.model.S3Object.builder()
            .key("data/orders.csv").size(30L).build();

        var page1 = ListObjectsV2Response.builder()
            .isTruncated(true)
            .nextContinuationToken("token-page2")
            .contents(obj1, obj2)
            .build();

        var page2 = ListObjectsV2Response.builder()
            .isTruncated(false)
            .contents(obj3)
            .build();

        var mockClient = mock(S3Client.class);
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenAnswer(inv -> {
                ListObjectsV2Request req = inv.getArgument(0);
                return "token-page2".equals(req.continuationToken()) ? page2 : page1;
            });

        var task = io.kestra.plugin.aws.s3.List.builder()
            .id("s3-service-list-pagination-filter-%s".formatted(IdUtils.create()))
            .type(io.kestra.plugin.aws.s3.List.class.getName())
            .bucket(Property.ofValue("my-bucket"))
            .regexp(Property.ofValue(".*\\.csv"))
            .maxKeys(Property.ofValue(2))
            .build();

        var result = S3Service.list(runContext, mockClient, task, task);

        assertThat(result, hasSize(1));
        assertThat(result.getFirst().getKey(), is("data/orders.csv"));
        // a regExp can skip most keys, so pages are requested at the S3 maximum instead of maxKeys
        verify(mockClient, times(2)).listObjectsV2(argThat((ListObjectsV2Request req) -> req.maxKeys() == 1000));
    }

    @Test
    void shouldStopPaginationWhenMatchingCountReachesMaxKeys() throws Exception {
        var runContext = runContextFactory.of();

        var obj1 = software.amazon.awssdk.services.s3.model.S3Object.builder()
            .key("orders-1.csv").size(10L).build();
        var obj2 = software.amazon.awssdk.services.s3.model.S3Object.builder()
            .key("orders-2.csv").size(20L).build();
        var obj3 = software.amazon.awssdk.services.s3.model.S3Object.builder()
            .key("orders-3.csv").size(30L).build();

        var page1 = ListObjectsV2Response.builder()
            .isTruncated(true)
            .nextContinuationToken("token-page2")
            .contents(obj1, obj2)
            .build();

        var page2 = ListObjectsV2Response.builder()
            .isTruncated(false)
            .contents(obj3)
            .build();

        var mockClient = mock(S3Client.class);
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenAnswer(inv -> {
                ListObjectsV2Request req = inv.getArgument(0);
                return "token-page2".equals(req.continuationToken()) ? page2 : page1;
            });

        var task = io.kestra.plugin.aws.s3.List.builder()
            .id("s3-service-list-max-keys-%s".formatted(IdUtils.create()))
            .type(io.kestra.plugin.aws.s3.List.class.getName())
            .bucket(Property.ofValue("my-bucket"))
            .regexp(Property.ofValue(".*\\.csv"))
            .maxKeys(Property.ofValue(2))
            .build();

        var result = S3Service.list(runContext, mockClient, task, task);

        assertThat(result, hasSize(2));
        assertThat(result.getFirst().getKey(), is("orders-1.csv"));
        assertThat(result.get(1).getKey(), is("orders-2.csv"));
        verify(mockClient, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void shouldRequestMaxKeysSizedPagesWithoutFilter() throws Exception {
        var mockClient = mock(S3Client.class);
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(ListObjectsV2Response.builder().isTruncated(false).build());
        var task = io.kestra.plugin.aws.s3.List.builder()
            .id("s3-service-list-page-size-%s".formatted(IdUtils.create()))
            .type(io.kestra.plugin.aws.s3.List.class.getName())
            .bucket(Property.ofValue("my-bucket"))
            .maxKeys(Property.ofValue(5))
            .build();

        S3Service.list(runContextFactory.of(), mockClient, task, task);

        verify(mockClient).listObjectsV2(argThat((ListObjectsV2Request req) -> req.maxKeys() == 5));
    }

    @Test
    void shouldReturnNothingWithoutCallingS3WhenMaxKeysIsZero() throws Exception {
        var mockClient = mock(S3Client.class);
        var task = io.kestra.plugin.aws.s3.List.builder()
            .id("s3-service-list-zero-max-keys-%s".formatted(IdUtils.create()))
            .type(io.kestra.plugin.aws.s3.List.class.getName())
            .bucket(Property.ofValue("my-bucket"))
            .maxKeys(Property.ofValue(0))
            .build();

        var result = S3Service.list(runContextFactory.of(), mockClient, task, task);

        assertThat(result, empty());
        verify(mockClient, never()).listObjectsV2(any(ListObjectsV2Request.class));
    }
}

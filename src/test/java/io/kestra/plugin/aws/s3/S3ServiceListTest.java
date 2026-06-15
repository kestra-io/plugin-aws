package io.kestra.plugin.aws.s3;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.s3.models.S3Object;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
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
            .thenAnswer(inv -> {
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
}

package io.kestra.plugin.aws.s3;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.AbstractConnectionInterface;
import io.kestra.plugin.aws.s3.models.S3Object;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class S3Service {
    public static Pair<GetObjectResponse, URI> download(RunContext runContext, S3AsyncClient client, GetObjectRequest.Builder builder) throws IOException, ExecutionException, InterruptedException {
        // s3 require non existing files
        File tempFile = runContext.tempFile().toFile();
        //noinspection ResultOfMethodCallIgnored
        tempFile.delete();

        try (S3TransferManager transferManager = S3TransferManager.builder().s3Client(client).build()) {
            FileDownload download = transferManager.downloadFile(
                DownloadFileRequest.builder()
                    .getObjectRequest(builder.build())
                    .destination(tempFile)
                    .build()
            );

            GetObjectResponse response =download.completionFuture().get().response();

            runContext.metric(Counter.of("file.size", response.contentLength()));

            return Pair.of(response, runContext.putTempFile(tempFile));
        }
    }

    static void archive(
        java.util.List<S3Object> s3Objects,
        ActionInterface.Action action,
        Copy.CopyObject moveTo,
        RunContext runContext,
        AbstractS3ObjectInterface abstractS3Object,
        AbstractConnectionInterface abstractS3,
        AbstractConnectionInterface abstractConnection
    ) throws Exception {
        if (action == ActionInterface.Action.DELETE) {
            for (S3Object object : s3Objects) {
                Delete delete = Delete.builder()
                    .id("archive")
                    .type(Delete.class.getName())
                    .region(abstractS3.getRegion())
                    .endpointOverride(abstractS3.getEndpointOverride())
                    .accessKeyId(abstractConnection.getAccessKeyId())
                    .secretKeyId(abstractConnection.getSecretKeyId())
                    .key(object.getKey())
                    .bucket(abstractS3Object.getBucket())
                    .build();
                delete.run(runContext);
            }
        } else if (action == ActionInterface.Action.MOVE) {
            for (S3Object object : s3Objects) {
                Copy copy = Copy.builder()
                    .id("archive")
                    .type(Copy.class.getName())
                    .region(abstractS3.getRegion())
                    .endpointOverride(abstractS3.getEndpointOverride())
                    .accessKeyId(abstractConnection.getAccessKeyId())
                    .secretKeyId(abstractConnection.getSecretKeyId())
                    .from(Copy.CopyObjectFrom.builder()
                        .bucket(abstractS3Object.getBucket())
                        .key(object.getKey())
                        .build()
                    )
                    .to(moveTo.toBuilder()
                        .key(StringUtils.stripEnd(moveTo.getKey() + "/", "/")
                            + "/" + FilenameUtils.getName(object.getKey())
                        )
                        .build()
                    )
                    .delete(true)
                    .build();
                copy.run(runContext);
            }
        }
    }

    public static List<S3Object> list(RunContext runContext, S3Client client, ListInterface list, AbstractS3Object abstractS3) throws IllegalVariableEvaluationException {
        ListObjectsRequest.Builder builder = ListObjectsRequest.builder()
            .bucket(runContext.render(list.getBucket()))
            .maxKeys(list.getMaxKeys());

        if (list.getPrefix() != null) {
            builder.prefix(runContext.render(list.getPrefix()));
        }

        if (list.getDelimiter() != null) {
            builder.delimiter(runContext.render(list.getDelimiter()));
        }

        if (list.getMarker() != null) {
            builder.marker(runContext.render(list.getMarker()));
        }

        if (list.getEncodingType() != null) {
            builder.encodingType(runContext.render(list.getEncodingType()));
        }

        if (list.getExpectedBucketOwner() != null) {
            builder.expectedBucketOwner(runContext.render(list.getExpectedBucketOwner()));
        }

        if (abstractS3.getRequestPayer() != null) {
            builder.requestPayer(runContext.render(abstractS3.getRequestPayer()));
        }

        String regExp = runContext.render(list.getRegexp());

        ListObjectsResponse listObjectsResponse = client.listObjects(builder.build());

        return listObjectsResponse
            .contents()
            .stream()
            .filter(s3Object -> S3Service.filter(s3Object, regExp, list.getFilter()))
            .map(S3Object::of)
            .collect(Collectors.toList());
    }

    private static boolean filter(software.amazon.awssdk.services.s3.model.S3Object object, String regExp, ListInterface.Filter filter) {
        return
            (regExp == null || object.key().matches(regExp)) &&
            (filter == ListInterface.Filter.BOTH ||
                (filter == ListInterface.Filter.DIRECTORY && object.key().endsWith("/")) ||
                (filter == ListInterface.Filter.FILES && !object.key().endsWith("/"))
            );
    }
}

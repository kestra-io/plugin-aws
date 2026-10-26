package io.kestra.plugin.aws.s3;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.aws.s3.models.S3Object;
import io.kestra.plugin.aws.shared.s3.AbstractS3;

import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

public class S3Service {

    public static void initCrt() {
        // This will init CRT and loads the native library
        CRT.getArchIdentifier();
    }

    public static Pair<GetObjectResponse, URI> download(RunContext runContext, S3AsyncClient client, GetObjectRequest request) throws IOException, ExecutionException, InterruptedException {
        // s3 require non existing files
        File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(request.key())).toFile();
        //noinspection ResultOfMethodCallIgnored
        tempFile.delete();

        try (S3TransferManager transferManager = S3TransferManager.builder().s3Client(client).build()) {
            FileDownload download = transferManager.downloadFile(
                DownloadFileRequest.builder()
                    .getObjectRequest(request)
                    .destination(tempFile)
                    .build()
            );

            GetObjectResponse response = download.completionFuture().get().response();

            runContext.metric(Counter.of("file.size", response.contentLength()));

            if (request.checksumMode() == ChecksumMode.ENABLED && extractChecksum(response).getLeft() == null) {
                runContext.logger().warn(
                    "Checksum validation requested for key '{}' but the object has no stored checksum; transfer was not verified.",
                    request.key()
                );
            }

            return Pair.of(response, runContext.storage().putFile(tempFile));
        }
    }

    /**
     * Returns the first non-null checksum from a {@link GetObjectResponse} as an (algorithm, base64-value) pair.
     * Both elements are null when the response carries no checksum.
     */
    public static Pair<String, String> extractChecksum(GetObjectResponse response) {
        if (response.checksumSHA256() != null) {
            return Pair.of("SHA256", response.checksumSHA256());
        }
        if (response.checksumSHA1() != null) {
            return Pair.of("SHA1", response.checksumSHA1());
        }
        if (response.checksumCRC32() != null) {
            return Pair.of("CRC32", response.checksumCRC32());
        }
        if (response.checksumCRC32C() != null) {
            return Pair.of("CRC32C", response.checksumCRC32C());
        }
        return Pair.of(null, null);
    }

    public static boolean isDirectoryMarker(S3Object object) {
        return object != null && isDirectoryMarker(object.getKey());
    }

    public static boolean isDirectoryMarker(String key) {
        return key != null && key.endsWith("/");
    }

    public static List<Pair<S3Object, String>> moveTargets(
        List<S3Object> s3Objects,
        Property<ActionInterface.Action> action,
        Copy.CopyObject moveTo,
        RunContext runContext,
        AbstractS3ObjectInterface abstractS3Object
    ) throws IllegalVariableEvaluationException {
        if (action == null || moveTo == null) {
            return List.of();
        }
        var renderedAction = runContext.render(action).as(ActionInterface.Action.class).orElse(null);
        if (renderedAction != ActionInterface.Action.MOVE) {
            return List.of();
        }

        var rMoveToKey = runContext.render(moveTo.getKey()).as(String.class).orElseThrow();
        var rPrefix = (abstractS3Object instanceof ListInterface listInterface && listInterface.getPrefix() != null)
            ? runContext.render(listInterface.getPrefix()).as(String.class).orElse(null)
            : null;

        var destinationMap = new HashMap<String, S3Object>();
        var targets = new ArrayList<Pair<S3Object, String>>();

        for (var object : s3Objects) {
            if (isDirectoryMarker(object)) {
                continue;
            }
            var targetKey = destinationKey(rMoveToKey, object.getKey(), rPrefix);
            var existing = destinationMap.put(targetKey, object);
            if (existing != null) {
                throw new IllegalStateException(
                    "Cannot move objects to '%s': multiple source objects would overwrite the same destination key: '%s' and '%s'. Set a `prefix` that covers both folders, or narrow the listing with `prefix` or `regExp`, so each object maps to its own key under `moveTo.key`."
                        .formatted(targetKey, existing.getKey(), object.getKey())
                );
            }
            targets.add(Pair.of(object, targetKey));
        }

        return targets;
    }

    static String destinationKey(String moveToKey, String objectKey, String prefix) {
        var destPrefix = StringUtils.stripEnd(moveToKey, "/");
        var subpath = "";

        if (prefix != null) {
            var normPrefix = StringUtils.strip(prefix, "/");
            var normKey = StringUtils.stripStart(objectKey, "/");
            if (!normPrefix.isEmpty()) {
                if (normKey.startsWith(normPrefix + "/")) {
                    subpath = normKey.substring(normPrefix.length() + 1);
                } else if (normPrefix.contains("/")) {
                    var folderBase = normPrefix.substring(0, normPrefix.lastIndexOf('/'));
                    if (!folderBase.isEmpty() && normKey.startsWith(folderBase + "/")) {
                        subpath = normKey.substring(folderBase.length() + 1);
                    }
                }
            }
        }

        if (subpath.isEmpty()) {
            subpath = FilenameUtils.getName(objectKey);
        }

        if (subpath.isEmpty()) {
            return destPrefix;
        }
        return destPrefix.isEmpty() ? subpath : destPrefix + "/" + subpath;
    }

    static void performAction(
        java.util.List<S3Object> s3Objects,
        Property<ActionInterface.Action> action,
        Copy.CopyObject moveTo,
        RunContext runContext,
        AbstractS3ObjectInterface abstractS3Object,
        AbstractS3 abstractS3) throws Exception {
        var renderedAction = runContext.render(action).as(ActionInterface.Action.class).orElseThrow();
        if (renderedAction == ActionInterface.Action.DELETE) {
            for (var object : s3Objects) {
                if (isDirectoryMarker(object)) {
                    continue;
                }
                var delete = Delete.builder()
                    .id("archive")
                    .type(Delete.class.getName())
                    .region(abstractS3.getRegion())
                    .endpointOverride(abstractS3.getEndpointOverride())
                    .compatibilityMode(abstractS3.getCompatibilityMode())
                    .forcePathStyle(abstractS3.getForcePathStyle())
                    .accessKeyId(abstractS3.getAccessKeyId())
                    .secretKeyId(abstractS3.getSecretKeyId())
                    .key(Property.ofValue(object.getKey()))
                    .bucket(abstractS3Object.getBucket())
                    .stsRoleArn(abstractS3.getStsRoleArn())
                    .stsRoleExternalId(abstractS3.getStsRoleExternalId())
                    .stsRoleSessionName(abstractS3.getStsRoleSessionName())
                    .stsRoleSessionDuration(abstractS3.getStsRoleSessionDuration())
                    .stsEndpointOverride(abstractS3.getStsEndpointOverride())
                    .build();
                delete.run(runContext);
            }
        } else if (renderedAction == ActionInterface.Action.MOVE) {
            var targets = moveTargets(s3Objects, action, moveTo, runContext, abstractS3Object);

            for (var target : targets) {
                var object = target.getLeft();
                var targetKey = target.getRight();
                var copy = Copy.builder()
                    .id("archive")
                    .type(Copy.class.getName())
                    .region(abstractS3.getRegion())
                    .endpointOverride(abstractS3.getEndpointOverride())
                    .compatibilityMode(abstractS3.getCompatibilityMode())
                    .forcePathStyle(abstractS3.getForcePathStyle())
                    .accessKeyId(abstractS3.getAccessKeyId())
                    .secretKeyId(abstractS3.getSecretKeyId())
                    .stsRoleArn(abstractS3.getStsRoleArn())
                    .stsRoleExternalId(abstractS3.getStsRoleExternalId())
                    .stsRoleSessionName(abstractS3.getStsRoleSessionName())
                    .stsRoleSessionDuration(abstractS3.getStsRoleSessionDuration())
                    .stsEndpointOverride(abstractS3.getStsEndpointOverride())
                    .from(
                        Copy.CopyObjectFrom.builder()
                            .bucket(abstractS3Object.getBucket())
                            .key(Property.ofValue(object.getKey()))
                            .build()
                    )
                    .to(
                        moveTo.toBuilder()
                            .key(Property.ofValue(targetKey))
                            .build()
                    )
                    .delete(Property.ofValue(true))
                    .build();
                copy.run(runContext);
            }
        }
    }

    public static List<S3Object> list(RunContext runContext, S3Client client, ListInterface list, AbstractS3Object abstractS3) throws IllegalVariableEvaluationException {
        int rMaxKeys = runContext.render(list.getMaxKeys()).as(Integer.class).orElse(1000);
        if (rMaxKeys < 0) {
            throw new IllegalArgumentException("The `maxKeys` property must be 0 or more, got %d.".formatted(rMaxKeys));
        }
        if (rMaxKeys == 0) {
            return List.of();
        }

        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
            .bucket(runContext.render(list.getBucket()).as(String.class).orElseThrow(() -> new IllegalArgumentException("The `bucket` property is required.")));

        if (list.getPrefix() != null) {
            builder.prefix(runContext.render(list.getPrefix()).as(String.class).orElseThrow());
        }

        if (list.getDelimiter() != null) {
            builder.delimiter(runContext.render(list.getDelimiter()).as(String.class).orElseThrow());
        }

        // V2 uses startAfter instead of marker (same "list after this key" semantics).
        if (list.getMarker() != null) {
            builder.startAfter(runContext.render(list.getMarker()).as(String.class).orElseThrow());
        }

        if (list.getEncodingType() != null) {
            builder.encodingType(runContext.render(list.getEncodingType()).as(String.class).orElseThrow());
        }

        if (list.getExpectedBucketOwner() != null) {
            builder.expectedBucketOwner(runContext.render(list.getExpectedBucketOwner()).as(String.class).orElseThrow());
        }

        if (abstractS3.getRequestPayer() != null) {
            builder.requestPayer(runContext.render(abstractS3.getRequestPayer()).as(String.class).orElseThrow());
        }

        var rPattern = runContext.render(list.getRegexp()).as(String.class).map(Pattern::compile).orElse(null);
        var rFilter = runContext.render(list.getFilter()).as(ListInterface.Filter.class).orElse(ListInterface.Filter.BOTH);

        // per-page size is capped at 1000 by S3, rMaxKeys is the total cap across pages, and a regexp or filter can skip most keys so it reads full pages
        builder.maxKeys(rPattern == null && rFilter == ListInterface.Filter.BOTH ? Math.min(rMaxKeys, 1000) : 1000);

        ListObjectsV2Request baseRequest = builder.build();
        var result = new ArrayList<S3Object>();

        ListObjectsV2Request currentRequest = baseRequest;
        ListObjectsV2Response response;
        do {
            response = client.listObjectsV2(currentRequest);
            response.contents().stream()
                .filter(s3Object -> filter(s3Object, rPattern, rFilter))
                .limit(rMaxKeys - result.size())
                .map(S3Object::of)
                .forEach(result::add);
            if (result.size() >= rMaxKeys) {
                return result;
            }

            if (response.isTruncated()) {
                String nextToken = response.nextContinuationToken();
                if (nextToken == null) {
                    // Guard: isTruncated=true but no token returned, stop to avoid an infinite loop.
                    runContext.logger().warn("S3 list pagination stopped: isTruncated=true but no continuation token available.");
                    break;
                }
                currentRequest = baseRequest.toBuilder().continuationToken(nextToken).build();
            }
        } while (response.isTruncated());

        return result;
    }

    private static boolean filter(software.amazon.awssdk.services.s3.model.S3Object object, Pattern pattern, ListInterface.Filter filter) {
        return (pattern == null || pattern.matcher(object.key()).matches()) &&
            (filter == ListInterface.Filter.BOTH ||
                (filter == ListInterface.Filter.DIRECTORY && object.key().endsWith("/")) ||
                (filter == ListInterface.Filter.FILES && !object.key().endsWith("/")));
    }
}

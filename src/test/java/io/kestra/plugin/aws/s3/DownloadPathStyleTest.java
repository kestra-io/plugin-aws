package io.kestra.plugin.aws.s3;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

@KestraTest
class DownloadPathStyleTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void multipleFilesListingUsesPathStyle() throws Exception {
        var paths = new CopyOnWriteArrayList<String>();
        var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            paths.add(exchange.getRequestURI().getPath());
            var body = "<ListBucketResult><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>"
                .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        try {
            var download = Download.builder()
                .id(IdUtils.create())
                .type(Download.class.getName())
                .bucket(Property.ofValue("my-bucket"))
                .prefix(Property.ofValue("folder/"))
                .endpointOverride(Property.ofValue("http://localhost:" + server.getAddress().getPort()))
                .accessKeyId(Property.ofValue("test"))
                .secretKeyId(Property.ofValue("test"))
                .region(Property.ofValue("us-east-1"))
                .forcePathStyle(Property.ofValue(true))
                .build();

            download.run(TestsUtils.mockRunContext(runContextFactory, download, Map.of()));

            assertThat(List.copyOf(paths), contains("/my-bucket"));
        } finally {
            server.stop(0);
        }
    }
}

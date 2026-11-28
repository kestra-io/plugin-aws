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
class DownloadsRequestHeadersTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void getObjectSendsRequestPayerAndExpectedBucketOwner() throws Exception {
        var getHeaders = new CopyOnWriteArrayList<String>();
        var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange ->
        {
            byte[] body;
            if (exchange.getRequestURI().getPath().endsWith("/file.txt")) {
                getHeaders.add(
                    exchange.getRequestHeaders().getFirst("x-amz-request-payer") + " " +
                        exchange.getRequestHeaders().getFirst("x-amz-expected-bucket-owner")
                );
                body = "content".getBytes(StandardCharsets.UTF_8);
            } else {
                body = ("<ListBucketResult><KeyCount>1</KeyCount><IsTruncated>false</IsTruncated>" +
                    "<Contents><Key>file.txt</Key><Size>7</Size></Contents></ListBucketResult>")
                    .getBytes(StandardCharsets.UTF_8);
            }
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        try {
            var downloads = Downloads.builder()
                .id(IdUtils.create())
                .type(Downloads.class.getName())
                .bucket(Property.ofValue("my-bucket"))
                .endpointOverride(Property.ofValue("http://localhost:" + server.getAddress().getPort()))
                .accessKeyId(Property.ofValue("test"))
                .secretKeyId(Property.ofValue("test"))
                .region(Property.ofValue("us-east-1"))
                .forcePathStyle(Property.ofValue(true))
                .compatibilityMode(Property.ofValue(true))
                .requestPayer(Property.ofValue("requester"))
                .expectedBucketOwner(Property.ofValue("123456789012"))
                .action(Property.ofValue(ActionInterface.Action.NONE))
                .build();

            downloads.run(TestsUtils.mockRunContext(runContextFactory, downloads, Map.of()));

            assertThat(List.copyOf(getHeaders), contains("requester 123456789012"));
        } finally {
            server.stop(0);
        }
    }
}

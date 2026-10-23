package io.kestra.plugin.aws.s3;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.s3.models.S3Object;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;

@KestraTest
class S3ServiceActionTest {
    private static final String SESSION_TOKEN = "temporary-session-token";

    @Inject
    private RunContextFactory runContextFactory;

    private HttpServer server;
    private final List<Map<String, String>> requests = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void deleteActionSendsSessionTokenAndRequestPayer() throws Exception {
        var task = downloads().action(Property.ofValue(ActionInterface.Action.DELETE)).build();

        S3Service.performAction(objects(), task.getAction(), null, runContextFactory.of(), task, task);

        assertThat(requests.stream().map(r -> r.get("operation")).toList(), contains("DeleteObject", "DeleteObject"));
        assertThat(requests, everyItem(hasEntry("token", SESSION_TOKEN)));
        assertThat(requests, everyItem(hasEntry("requestPayer", "requester")));
    }

    @Test
    void moveActionSendsSessionToken() throws Exception {
        var task = downloads()
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveTo(Copy.CopyObject.builder().bucket(Property.ofValue("archive")).key(Property.ofValue("archived")).build())
            .build();

        S3Service.performAction(objects(), task.getAction(), task.getMoveTo(), runContextFactory.of(), task, task);

        assertThat(
            requests.stream().map(r -> r.get("operation")).toList(),
            contains("CopyObject", "DeleteObject", "CopyObject", "DeleteObject")
        );
        assertThat(requests, everyItem(hasEntry("token", SESSION_TOKEN)));
    }

    private Downloads.DownloadsBuilder<?, ?> downloads() {
        return Downloads.builder()
            .id("s3-service-action")
            .type(Downloads.class.getName())
            .endpointOverride(Property.ofValue("http://127.0.0.1:" + server.getAddress().getPort()))
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("ASIATEMPORARYKEY"))
            .secretKeyId(Property.ofValue("temporary-secret"))
            .sessionToken(Property.ofValue(SESSION_TOKEN))
            .requestPayer(Property.ofValue("requester"))
            .forcePathStyle(Property.ofValue(true))
            .compatibilityMode(Property.ofValue(true))
            .bucket(Property.ofValue("source"));
    }

    private static List<S3Object> objects() {
        return List.of(S3Object.builder().key("in/a.txt").build(), S3Object.builder().key("in/b.txt").build());
    }

    private void handle(HttpExchange exchange) throws IOException {
        var headers = exchange.getRequestHeaders();
        var isCopy = "PUT".equals(exchange.getRequestMethod()) && headers.containsKey("x-amz-copy-source");
        requests.add(
            Map.of(
                "operation", isCopy ? "CopyObject" : "DELETE".equals(exchange.getRequestMethod()) ? "DeleteObject" : exchange.getRequestMethod(),
                "token", String.valueOf(headers.getFirst("x-amz-security-token")),
                "requestPayer", String.valueOf(headers.getFirst("x-amz-request-payer"))
            )
        );
        exchange.getRequestBody().readAllBytes();

        if (isCopy) {
            var body = "<CopyObjectResult><ETag>\"etag\"</ETag><LastModified>2026-01-01T00:00:00.000Z</LastModified></CopyObjectResult>"
                .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/xml");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
        } else {
            exchange.sendResponseHeaders(204, -1);
        }
        exchange.close();
    }
}

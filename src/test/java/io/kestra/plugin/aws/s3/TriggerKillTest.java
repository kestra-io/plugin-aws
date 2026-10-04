package io.kestra.plugin.aws.s3;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class TriggerKillTest {
    @Inject
    private RunContextFactory runContextFactory;

    // accepts connections but never answers, so every SDK call hangs until its socket timeout
    private ServerSocket hangingServer;
    private final List<Socket> accepted = new CopyOnWriteArrayList<>();
    private final CountDownLatch connected = new CountDownLatch(1);

    @BeforeEach
    void startHangingServer() throws IOException {
        hangingServer = new ServerSocket(0);
        Thread.ofPlatform().daemon().start(() ->
        {
            try {
                while (!hangingServer.isClosed()) {
                    accepted.add(hangingServer.accept());
                    connected.countDown();
                }
            } catch (IOException ignored) {
                // server closed
            }
        });
    }

    @AfterEach
    void stopHangingServer() throws IOException {
        hangingServer.close();
        for (Socket socket : accepted) {
            socket.close();
        }
    }

    @Test
    void killReleasesHungEvaluation() throws Exception {
        var endpoint = "http://localhost:" + hangingServer.getLocalPort();
        var trigger = Trigger.builder()
            .id("s3-kill")
            .type(Trigger.class.getName())
            .endpointOverride(Property.ofValue(endpoint))
            .bucket(Property.ofValue("hanging-bucket"))
            .forcePathStyle(Property.ofValue(true))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test"))
            .secretKeyId(Property.ofValue("test"))
            .interval(Duration.ofSeconds(60))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var evaluator = Executors.newSingleThreadExecutor();
        var evaluation = CompletableFuture.supplyAsync(() ->
        {
            try {
                return trigger.evaluate(context.getKey(), context.getValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, evaluator);

        // wait until the S3 list call is actually in flight against the hanging server
        assertTrue(connected.await(30, TimeUnit.SECONDS), "the S3 list call never reached the server");
        trigger.kill();

        // without kill() this would block for at least the SDK socket timeout (30s) plus retries
        var thrown = assertThrows(ExecutionException.class, () -> evaluation.get(5, TimeUnit.SECONDS));
        assertThat(thrown.getCause().getCause(), instanceOf(CancellationException.class));
        evaluator.shutdownNow();
    }

    @Test
    void killWithoutEvaluationIsNoop() {
        var trigger = Trigger.builder()
            .id("s3-kill-noop")
            .type(Trigger.class.getName())
            .build();

        trigger.kill();
    }
}

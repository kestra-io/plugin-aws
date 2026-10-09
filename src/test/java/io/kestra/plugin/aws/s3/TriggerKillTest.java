package io.kestra.plugin.aws.s3;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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
import static org.hamcrest.Matchers.is;
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
        var trigger = hangingTrigger();
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

        // without kill() this would block for at least the SDK socket timeout (30s) plus retries;
        // a killed evaluation is a clean stop, not a trigger error
        assertThat(evaluation.get(5, TimeUnit.SECONDS).isEmpty(), is(true));
        evaluator.shutdownNow();

        // the SDK request itself must be aborted, not only the wait on it
        var socket = accepted.get(0);
        socket.setSoTimeout(5_000);
        var in = socket.getInputStream();
        var buf = new byte[8192];
        try {
            while (in.read(buf) != -1) {
            }
        } catch (SocketTimeoutException e) {
            throw new AssertionError("the SDK request is still open after kill()", e);
        } catch (SocketException e) {
        }
    }

    @Test
    void killBeforeEvaluationSkipsIt() throws Exception {
        var trigger = hangingTrigger();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.kill();

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isEmpty(), is(true));
        assertThat(accepted.isEmpty(), is(true));
    }

    @Test
    void killWithoutEvaluationIsNoop() {
        var trigger = Trigger.builder()
            .id("s3-kill-noop")
            .type(Trigger.class.getName())
            .build();

        trigger.kill();
    }

    private Trigger hangingTrigger() {
        var endpoint = "http://localhost:" + hangingServer.getLocalPort();
        return Trigger.builder()
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
    }
}

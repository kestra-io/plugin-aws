package io.kestra.plugin.aws.utils;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.utils.SdkAutoCloseable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KillableClientHolderTest {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(KillableClientHolderTest.class);

    @Test
    void runClosesClientOnce() throws Exception {
        var holder = new KillableClientHolder();
        var closes = new AtomicInteger();

        var result = holder.run(() -> (SdkAutoCloseable) closes::incrementAndGet, client -> "done");

        assertThat(result, is("done"));
        assertThat(closes.get(), is(1));
    }

    @Test
    void killBeforeRunDoesNotCreateClient() {
        var holder = new KillableClientHolder();
        var created = new AtomicBoolean();
        holder.kill();

        assertThrows(TriggerKilledException.class, () -> holder.run(() ->
        {
            created.set(true);
            return () ->
            {
            };
        }, client -> "never"));
        assertThat(created.get(), is(false));
    }

    @Test
    void killDuringCallClosesClientAndStopsCleanly() throws Exception {
        var holder = new KillableClientHolder();
        var inCall = new CountDownLatch(1);
        var closed = new CountDownLatch(1);
        var closes = new AtomicInteger();
        var outcome = new AtomicReference<Object>();

        var evaluation = Thread.ofPlatform().start(() ->
        {
            try {
                var result = holder.evaluate(LOGGER, () -> Optional.of(holder.run(() -> (SdkAutoCloseable) () ->
                {
                    closes.incrementAndGet();
                    closed.countDown();
                }, client ->
                {
                    inCall.countDown();
                    // mimics an SDK call that fails once its client is closed
                    closed.await();
                    throw new IllegalStateException("connection closed");
                })));
                outcome.set(result);
            } catch (Exception e) {
                outcome.set(e);
            }
        });

        assertThat(inCall.await(5, TimeUnit.SECONDS), is(true));
        holder.kill();
        evaluation.join(5_000);

        assertThat(evaluation.isAlive(), is(false));
        // evaluate() turned the kill into an empty result instead of an error
        assertThat(outcome.get(), is(Optional.empty()));
        assertThat(closes.get(), is(1));
    }

    @Test
    void failureWithoutKillIsRethrown() {
        var holder = new KillableClientHolder();

        var thrown = assertThrows(IllegalStateException.class, () -> holder.runWithoutClient(() ->
        {
            throw new IllegalStateException("boom");
        }));
        assertThat(thrown.getMessage(), is("boom"));
    }

    @Test
    void failureAfterKillBecomesCleanStop() {
        var holder = new KillableClientHolder();
        holder.kill();

        assertThrows(TriggerKilledException.class, () -> holder.runWithoutClient(() ->
        {
            throw new IllegalStateException("aborted by interrupt");
        }));
    }
}

package io.kestra.plugin.aws.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class KillableClientsTest {
    @Test
    void interruptDuringCloseDoesNotReachClient() throws Exception {
        var closeStarted = new CountDownLatch(1);
        var closeCompleted = new AtomicBoolean();
        var closeInterrupted = new AtomicBoolean();
        var callerInterruptedAfter = new AtomicBoolean();

        // mimics a Netty client close waiting for its event loop to shut down
        var caller = Thread.ofPlatform().start(() ->
        {
            KillableClients.closeQuietly(() ->
            {
                closeStarted.countDown();
                try {
                    Thread.sleep(500);
                    closeCompleted.set(true);
                } catch (InterruptedException e) {
                    closeInterrupted.set(true);
                }
            });
            callerInterruptedAfter.set(Thread.currentThread().isInterrupted());
        });

        assertThat(closeStarted.await(5, TimeUnit.SECONDS), is(true));
        caller.interrupt();
        caller.join(5_000);

        assertThat(closeInterrupted.get(), is(false));
        assertThat(closeCompleted.get(), is(true));
        // the interrupt is not swallowed, only deferred until the close is done
        assertThat(callerInterruptedAfter.get(), is(true));
    }

    @Test
    void closeFailureIsSwallowed() {
        KillableClients.closeQuietly(() ->
        {
            throw new IllegalStateException("boom");
        });
    }
}

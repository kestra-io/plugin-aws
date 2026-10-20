package io.kestra.plugin.aws.utils;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import io.kestra.core.utils.Rethrow;

import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Kill handling shared by the polling triggers: holds the AWS client of the call in flight so {@link #kill()} can close
 * it, which aborts the request, and a sticky killed flag so a kill that lands between calls is not lost.
 * <p>
 * Triggers keep one instance in a {@code transient} field, so core's {@code EqualsBuilder.reflectionEquals} trigger
 * change detection ignores it.
 */
public final class KillableClientHolder {
    private final AtomicBoolean killed = new AtomicBoolean();
    private final AtomicReference<SdkAutoCloseable> activeClient = new AtomicReference<>();

    /**
     * Runs a trigger evaluation and turns a kill into a clean stop, so the worker does not report it as a trigger error.
     */
    public <T> Optional<T> evaluate(Logger logger, Rethrow.CallableChecked<Optional<T>, Exception> evaluation) throws Exception {
        try {
            return evaluation.call();
        } catch (TriggerKilledException e) {
            logger.debug("Trigger killed, evaluation stopped");
            return Optional.empty();
        }
    }

    /** The client is published so {@link #kill()} can close it and abort the in-flight request. */
    public <C extends SdkAutoCloseable, T> T run(
        Rethrow.SupplierChecked<C, Exception> clientFactory,
        Rethrow.FunctionChecked<C, T, Exception> call) throws Exception {
        throwIfKilled();

        var client = clientFactory.get();
        activeClient.set(client);
        try {
            // a kill between the check above and publishing the client found no client to close
            throwIfKilled();
            return call.apply(client);
        } catch (TriggerKilledException e) {
            throw e;
        } catch (Exception e) {
            throw killedOr(e);
        } finally {
            if (activeClient.compareAndSet(client, null)) {
                KillableClients.closeQuietly(client);
            }
        }
    }

    /**
     * Runs an action that does not use a killable client, and turns a failure caused by a kill (the worker interrupts
     * the evaluation thread after {@link #kill()}) into a clean stop.
     */
    public void runWithoutClient(Rethrow.RunnableChecked<Exception> action) throws Exception {
        try {
            action.run();
        } catch (Exception e) {
            throw killedOr(e);
        }
    }

    public void throwIfKilled() {
        if (killed.get()) {
            throw new TriggerKilledException();
        }
    }

    /**
     * Marks the trigger as killed and closes the client in flight, if any, without waiting: the worker calls
     * {@code kill()} while holding its own lock.
     */
    public void kill() {
        killed.set(true);
        var client = activeClient.getAndSet(null);
        if (client != null) {
            KillableClients.closeInBackground(client);
        }
    }

    private Exception killedOr(Exception e) {
        if (!killed.get()) {
            return e;
        }

        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new TriggerKilledException();
    }
}

package io.kestra.plugin.aws.utils;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.utils.SdkAutoCloseable;

@Slf4j
public final class KillableClients {
    private KillableClients() {
    }

    /**
     * Closes an AWS SDK client on a separate thread and waits for it, without letting an interrupt reach the close.
     * On kill the worker interrupts the evaluation thread, and a Netty async client interrupted while waiting for its
     * event loop to shut down logs ERROR traces. The interrupt is deferred until the close is done, then restored.
     */
    public static void closeQuietly(SdkAutoCloseable client) {
        var closer = closeInBackground(client);

        var interrupted = false;
        while (true) {
            try {
                closer.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public static Thread closeInBackground(SdkAutoCloseable client) {
        return Thread.ofVirtual().start(() ->
        {
            try {
                client.close();
            } catch (RuntimeException e) {
                log.debug("Failed to close AWS client of a killed trigger", e);
            }
        });
    }
}

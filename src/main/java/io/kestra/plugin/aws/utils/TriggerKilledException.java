package io.kestra.plugin.aws.utils;

/**
 * Thrown inside a polling trigger evaluation once the trigger has been killed, so the evaluation can stop cleanly
 * instead of being reported as a trigger error.
 */
public class TriggerKilledException extends RuntimeException {
    public TriggerKilledException() {
        super("Trigger was killed", null, false, false);
    }
}

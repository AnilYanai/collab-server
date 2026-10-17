package com.anilyanai.connector.events;

/**
 * Functional callback listener notified when a client connection disconnects or drops.
 */
@FunctionalInterface
public interface DisconnectListener {
    void onDisconnect(String connectionId);
}

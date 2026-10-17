package com.anilyanai.business.broadcast;

import com.anilyanai.protocol.Packet;

/**
 * Functional abstraction for emitting raw pre-encoded or structured packets to target connections.
 */
@FunctionalInterface
public interface MessagePublisher {

    /**
     * Emits pre-encoded raw frame bytes to a target connection.
     *
     * @param connectionId destination connection identifier (client or peer)
     * @param rawBytes     complete binary packet frame
     */
    void publish(String connectionId, byte[] rawBytes);

    /**
     * Convenience default method for emitting an object / Packet when raw bytes are not pre-encoded.
     */
    default void publish(String connectionId, Packet packet) {
        // Can be overridden or handled by dispatcher codec
    }
}

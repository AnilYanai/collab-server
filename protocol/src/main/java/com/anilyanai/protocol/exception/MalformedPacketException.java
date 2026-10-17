package com.anilyanai.protocol.exception;

/**
 * Thrown when a wire packet violates the protocol specification or contains malformed data.
 */
public class MalformedPacketException extends RuntimeException {
    public MalformedPacketException(String message) {
        super(message);
    }

    public MalformedPacketException(String message, Throwable cause) {
        super(message, cause);
    }
}

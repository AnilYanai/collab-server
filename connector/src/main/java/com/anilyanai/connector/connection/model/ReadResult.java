package com.anilyanai.connector.connection.model;

import java.net.SocketAddress;
import java.nio.ByteBuffer;

public class ReadResult {

    private final ByteBuffer buffer;

    private final SocketAddress sourceAddress;

    public ReadResult(
            ByteBuffer buffer,
            SocketAddress sourceAddress
    ) {
        this.buffer = buffer;
        this.sourceAddress = sourceAddress;
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public SocketAddress sourceAddress() {
        return sourceAddress;
    }
}

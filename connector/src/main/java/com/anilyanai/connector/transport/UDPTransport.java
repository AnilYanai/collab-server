package com.anilyanai.connector.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

public class UDPTransport {

    private static final int BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB OS kernel socket buffer

    private DatagramChannel channel;

    public void initialize() throws IOException {
        channel = DatagramChannel.open();
        channel.configureBlocking(false);

        // Expand OS kernel receive & send buffers to handle high-frequency bursts without dropping packets
        channel.setOption(StandardSocketOptions.SO_RCVBUF, BUFFER_SIZE);
        channel.setOption(StandardSocketOptions.SO_SNDBUF, BUFFER_SIZE);
    }

    public void bind(int port) throws IOException {
        channel.bind(new InetSocketAddress(port));
    }

    public void register(Selector selector) throws IOException {
        channel.register(
                selector,
                SelectionKey.OP_READ
        );
    }

    public DatagramChannel getChannel() {
        return channel;
    }
}

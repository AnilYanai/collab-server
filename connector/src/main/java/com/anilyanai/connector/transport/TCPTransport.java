package com.anilyanai.connector.transport;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

public class TCPTransport {

    private ServerSocketChannel serverChannel;

    public void initialize() throws IOException {
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);

        /*
         * Enable SO_REUSEADDR:
         * Allows immediate rebinding to the server port even if previous sockets
         * remain in OS TIME_WAIT state, preventing 'Address already in use' errors
         * during fast restarts or consecutive test/benchmark runs.
         */
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    }

    public void bind(int port) throws IOException {
        /*
         * Connection Backlog (4,096):
         * Supports massive connection storms (e.g. 2,500+ clients connecting simultaneously)
         * without OS kernel TCP SYN drops or 'Connection refused' errors.
         */
        serverChannel.bind(
                new InetSocketAddress(port),
                4096
        );
    }

    public void register(Selector selector)
            throws IOException {

        serverChannel.register(
                selector,
                SelectionKey.OP_ACCEPT
        );
    }

    public ServerSocketChannel getChannel() {
        return serverChannel;
    }
}

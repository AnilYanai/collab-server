package com.anilyanai.connector.selector;

import com.anilyanai.connector.configuration.ConnectorProperties;
import com.anilyanai.connector.connection.TCPConnection;
import com.anilyanai.connector.events.InboundMessageHandler;
import com.anilyanai.connector.executor.WorkerPool;
import com.anilyanai.connector.registry.ConnectionRegistry;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * =========================================================================================
 * 🚀 TCPInboundTrafficSelector (Dedicated High-Performance TCP Inbound Reactor)
 * =========================================================================================
 *
 * <p>Dedicated single-threaded Java NIO reactor responsible exclusively for TCP network events:
 * accepting new client connections, reading length-prefixed binary frames, and detecting resets.</p>
 */
@Slf4j
public class TCPInboundTrafficSelector {

    private static final int DEFAULT_BATCH_SIZE = 64;

    @Getter
    private Selector selector;
    private volatile boolean running = true;
    private Thread selectorThread;

    private final ConnectionRegistry connectionRegistry;
    private final WorkerPool workerPool;
    private InboundMessageHandler inboundMessageHandler;

    @Setter
    private ConnectorProperties properties;

    @Setter
    private OutboundTrafficSelector outboundTrafficSelector;

    private final ByteBuffer sharedReadBuffer = ByteBuffer.allocateDirect(64 * 1024);

    private record InboundTCPFrame(String connectionId, byte[] frame) {}

    public TCPInboundTrafficSelector(ConnectionRegistry connectionRegistry, WorkerPool workerPool) {
        this(connectionRegistry, workerPool, null);
    }

    public TCPInboundTrafficSelector(ConnectionRegistry connectionRegistry, WorkerPool workerPool, ConnectorProperties properties) {
        this.connectionRegistry = connectionRegistry;
        this.workerPool = workerPool;
        this.properties = properties;
    }

    public void initialize(InboundMessageHandler inboundMessageHandler) throws IOException {
        this.inboundMessageHandler = inboundMessageHandler;
        this.selector = Selector.open();
    }

    public void registerServerChannel(ServerSocketChannel serverChannel) throws IOException {
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    public void start() {
        selectorThread = new Thread(this::eventLoop, "tcp-inbound-selector");
        selectorThread.start();
        log.info("🚀 TCP Inbound Selector started (dedicated NIO reactor thread, batch size: {})", getEffectiveBatchSize());
    }

    public void stop() {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }
        try {
            if (selectorThread != null) {
                selectorThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private int getEffectiveBatchSize() {
        return (properties != null && properties.getTcpInboundBatchSize() > 0)
                ? properties.getTcpInboundBatchSize()
                : DEFAULT_BATCH_SIZE;
    }

    private void eventLoop() {
        final int batchSize = getEffectiveBatchSize();
        List<InboundTCPFrame> batch = new ArrayList<>(batchSize);

        while (running) {
            try {
                int readyCount = selector.select(500);
                if (readyCount == 0) continue;

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid()) continue;

                    if (key.isAcceptable()) {
                        acceptClient((ServerSocketChannel) key.channel());
                    } else if (key.isReadable()) {
                        batch = readClient(key, batch, batchSize);
                    }
                }

                if (!batch.isEmpty()) {
                    dispatchBatch(batch);
                    batch = new ArrayList<>(batchSize);
                }
            } catch (Exception e) {
                if (!running) break;
            }
        }
    }

    private void dispatchBatch(List<InboundTCPFrame> batch) {
        workerPool.submit(() -> {
            for (InboundTCPFrame item : batch) {
                inboundMessageHandler.onReceive(item.connectionId(), item.frame());
            }
        });
    }

    private void acceptClient(ServerSocketChannel serverChannel) throws IOException {
        SocketChannel clientChannel;
        while ((clientChannel = serverChannel.accept()) != null) {
            clientChannel.configureBlocking(false);
            clientChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            clientChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

            int sendBuf = (properties != null && properties.getTcpSendBufferSize() > 0)
                    ? properties.getTcpSendBufferSize()
                    : 128 * 1024;
            int rcvBuf = (properties != null && properties.getTcpReceiveBufferSize() > 0)
                    ? properties.getTcpReceiveBufferSize()
                    : 128 * 1024;

            clientChannel.setOption(StandardSocketOptions.SO_SNDBUF, sendBuf);
            clientChannel.setOption(StandardSocketOptions.SO_RCVBUF, rcvBuf);

            String connectionId = UUID.randomUUID().toString();
            TCPConnection connection = new TCPConnection(connectionId, clientChannel);

            // Pre-assign outbound reactor ONCE on accept to eliminate per-packet hashing & branching
            if (outboundTrafficSelector != null) {
                OutboundTrafficSelector.OutboundReactor assignedReactor =
                        outboundTrafficSelector.getAssignedReactor(connection.getConnectionIndex());
                connection.setAssignedReactor(assignedReactor);

                // ⚡ Pre-register with assigned outbound reactor with interest 0 and cache SelectionKey!
                // Eliminates blocking channel.register() and channel.keyFor() locks on the broadcast hot path!
                if (assignedReactor != null) {
                    try {
                        SelectionKey outboundKey = assignedReactor.registerConnection(connection);
                        connection.setOutboundKey(outboundKey);
                    } catch (ClosedChannelException ignored) {
                    }
                }
            }

            connectionRegistry.add(connection);
            clientChannel.register(selector, SelectionKey.OP_READ, connection);
        }
    }

    private List<InboundTCPFrame> readClient(SelectionKey key, List<InboundTCPFrame> batch, int batchSize) {
        TCPConnection connection = (TCPConnection) key.attachment();
        if (connection == null) return batch;
        SocketChannel clientChannel = (SocketChannel) key.channel();

        int bytesRead;
        try {
            sharedReadBuffer.clear();
            bytesRead = clientChannel.read(sharedReadBuffer);
        } catch (IOException e) {
            log.debug("TCP Client {} reset/aborted abruptly: {}", connection.getConnectionId(), e.getMessage());
            closeAndCleanupConnection(key, clientChannel, connection.getConnectionId());
            return batch;
        }

        if (bytesRead == -1) {
            closeAndCleanupConnection(key, clientChannel, connection.getConnectionId());
            return batch;
        }

        if (bytesRead > 0) {
            sharedReadBuffer.flip();
            List<byte[]> frames = connection.extractFrames(sharedReadBuffer);
            if (!frames.isEmpty()) {
                String connectionId = connection.getConnectionId();
                for (byte[] frame : frames) {
                    batch.add(new InboundTCPFrame(connectionId, frame));
                    if (batch.size() == batchSize) {
                        dispatchBatch(batch);
                        batch = new ArrayList<>(batchSize);
                    }
                }
            }
        }
        return batch;
    }

    private void closeAndCleanupConnection(SelectionKey key, SocketChannel clientChannel, String connectionId) {
        try {
            connectionRegistry.removeConnection(connectionId);
            key.cancel();
            clientChannel.close();
        } catch (IOException e) {
            log.debug("Error closing TCP client channel for {}: {}", connectionId, e.getMessage());
        }
    }
}

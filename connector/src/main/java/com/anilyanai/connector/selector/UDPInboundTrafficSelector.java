package com.anilyanai.connector.selector;

import com.anilyanai.connector.configuration.ConnectorProperties;
import com.anilyanai.connector.connection.UDPConnection;
import com.anilyanai.connector.events.InboundMessageHandler;
import com.anilyanai.connector.executor.WorkerPool;
import com.anilyanai.connector.registry.ConnectionRegistry;
import com.anilyanai.connector.transport.UDPEgressPool;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * =========================================================================================
 * 🚀 UDPInboundTrafficSelector (Dedicated Wire-Speed UDP Inbound Reactor)
 * =========================================================================================
 *
 * <p>Dedicated single-threaded Java NIO reactor responsible exclusively for UDP datagram ingestion.</p>
 */
@Slf4j
public class UDPInboundTrafficSelector {

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
    private UDPEgressPool udpEgressPool;

    private DatagramChannel udpChannel;
    private final ByteBuffer sharedReadBuffer = ByteBuffer.allocateDirect(64 * 1024);

    private record InboundPacket(String connectionId, byte[] data) {}

    public UDPInboundTrafficSelector(ConnectionRegistry connectionRegistry, WorkerPool workerPool) {
        this(connectionRegistry, workerPool, null);
    }

    public UDPInboundTrafficSelector(ConnectionRegistry connectionRegistry, WorkerPool workerPool, ConnectorProperties properties) {
        this.connectionRegistry = connectionRegistry;
        this.workerPool = workerPool;
        this.properties = properties;
    }

    public void initialize(InboundMessageHandler inboundMessageHandler) throws IOException {
        this.inboundMessageHandler = inboundMessageHandler;
        this.selector = Selector.open();
    }

    public void registerChannel(DatagramChannel channel) throws IOException {
        this.udpChannel = channel;
        channel.register(selector, SelectionKey.OP_READ);
    }

    public void start() {
        selectorThread = new Thread(this::eventLoop, "udp-inbound-selector");
        selectorThread.start();
        log.info("🚀 UDP Inbound Selector started (dedicated wire-speed NIO reactor thread, batch size: {})", getEffectiveBatchSize());
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
        return (properties != null && properties.getUdpInboundBatchSize() > 0)
                ? properties.getUdpInboundBatchSize()
                : DEFAULT_BATCH_SIZE;
    }

    private void eventLoop() {
        final int batchSize = getEffectiveBatchSize();
        List<InboundPacket> batch = new ArrayList<>(batchSize);

        while (running) {
            try {
                if (selector.select(500) == 0) continue;
                selector.selectedKeys().clear();

                sharedReadBuffer.clear();
                SocketAddress source;

                while ((source = udpChannel.receive(sharedReadBuffer)) != null) {
                    sharedReadBuffer.flip();

                    UDPConnection connection = getOrRegisterUDPConnection((InetSocketAddress) source);
                    byte[] data = new byte[sharedReadBuffer.remaining()];
                    sharedReadBuffer.get(data);
                    batch.add(new InboundPacket(connection.getConnectionId(), data));

                    if (batch.size() == batchSize) {
                        dispatchBatch(batch);
                        batch = new ArrayList<>(batchSize);
                    }

                    sharedReadBuffer.clear();
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

    private void dispatchBatch(List<InboundPacket> batch) {
        workerPool.submit(() -> {
            for (InboundPacket packet : batch) {
                inboundMessageHandler.onReceive(packet.connectionId(), packet.data());
            }
        });
    }

    private UDPConnection getOrRegisterUDPConnection(InetSocketAddress address) {
        UDPConnection connection = connectionRegistry.getUDPConnection(address);
        if (connection != null) return connection;

        connection = new UDPConnection(
                UUID.randomUUID().toString(),
                udpChannel,
                address
        );

        if (udpEgressPool != null) {
            connection.setEgressChannel(udpEgressPool.getAssignedChannel(connection.getConnectionIndex()));
        }

        connectionRegistry.add(connection);
        return connection;
    }
}

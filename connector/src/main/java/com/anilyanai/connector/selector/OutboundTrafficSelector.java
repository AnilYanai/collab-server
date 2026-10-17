package com.anilyanai.connector.selector;

import com.anilyanai.connector.connection.Connection;
import com.anilyanai.connector.connection.TCPConnection;
import com.anilyanai.connector.events.OutboundMessageHandler;
import com.anilyanai.connector.registry.ConnectionRegistry;
import com.anilyanai.connector.transport.UDPEgressPool;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

/**
 * =========================================================================================
 * 🚀 OutboundTrafficSelector (Configurable Multi-Reactor Java NIO Outbound Drain Array)
 * =========================================================================================
 *
 * <p>The {@code OutboundTrafficSelector} coordinates an array of non-blocking Java NIO selectors
 * responsible for asynchronously draining outbound message queues and transmitting binary frames.</p>
 *
 * <h3>Key Architectural Innovations:</h3>
 * <ul>
 *   <li><b>100% Branchless & Zero-Instanceof Hot Path:</b> Outbound packet dispatching is
 *       completely polymorphic via {@link Connection#sendOutboundPayload(byte[])}. Eliminates
 *       all runtime {@code instanceof} checks, type-casting, and repeated hashing from the per-packet path.</li>
 *   <li><b>Pre-Cached Affinity:</b> UDP egress channels and TCP outbound reactors are assigned
 *       <b>EXACTLY ONCE</b> on connection registration and cached directly on the connection instance.</li>
 *   <li><b>Multi-Reactor Array (1 to N Selectors):</b> Configurable via
 *       {@code collab.connector.outbound-selectors} in {@code application.properties}.</li>
 *   <li><b>Zero-Delay Non-Blocking Drain:</b> Uses {@code selectNow()} for immediate line-rate draining,
 *       1ms polling while draining backlogs, and sleeps cleanly with zero CPU utilization when idle.</li>
 *   <li><b>Fair Gathering Batch Writes:</b> Flushes up to 128 queued message buffers in a single kernel syscall.</li>
 *   <li><b>High-Performance Write Interest Management:</b> Disables {@code OP_WRITE} atomically as soon
 *       as the connection write queue is truly drained, and uses a lock-free double-check pattern to
 *       eliminate race conditions with concurrent worker thread enqueue operations.</li>
 * </ul>
 *
 * @author anilyanai
 */
@Slf4j
public class OutboundTrafficSelector {

    @Getter
    private final int selectorCount;
    private final OutboundReactor[] reactors;
    private final ConnectionRegistry connectionRegistry;
    @Getter
    private final UDPEgressPool udpEgressPool;
    private OutboundMessageHandler outboundMessageHandler;

    public OutboundTrafficSelector(ConnectionRegistry connectionRegistry) {
        this(connectionRegistry, 1, null);
    }

    public OutboundTrafficSelector(ConnectionRegistry connectionRegistry, int selectorCount) {
        this(connectionRegistry, selectorCount, null);
    }

    public OutboundTrafficSelector(ConnectionRegistry connectionRegistry, int selectorCount, UDPEgressPool udpEgressPool) {
        this.connectionRegistry = connectionRegistry;
        this.selectorCount = Math.max(1, selectorCount);
        this.udpEgressPool = udpEgressPool;
        this.reactors = new OutboundReactor[this.selectorCount];
        for (int i = 0; i < this.selectorCount; i++) {
            this.reactors[i] = new OutboundReactor(i);
        }
    }

    public void initialize(OutboundMessageHandler outboundMessageHandler) throws IOException {
        this.outboundMessageHandler = outboundMessageHandler;
        for (OutboundReactor reactor : reactors) {
            reactor.initialize();
        }
        if (udpEgressPool != null) {
            udpEgressPool.initialize();
        }
    }

    public void start() {
        for (OutboundReactor reactor : reactors) {
            reactor.start();
        }
        if (selectorCount == 1) {
            log.info("🚀 Outbound Selector started (1 dedicated NIO reactor thread)");
        } else {
            log.info("🚀 Outbound Selector array started ({} dedicated NIO reactor threads)", selectorCount);
        }
    }

    public void stop() {
        for (OutboundReactor reactor : reactors) {
            reactor.stop();
        }
        if (udpEgressPool != null) {
            udpEgressPool.close();
        }
    }

    /**
     * Resolves the assigned outbound reactor for a connection ONCE during connection creation.
     */
    public OutboundReactor getAssignedReactor(int connectionIndex) {
        if (reactors == null || reactors.length == 0) return null;
        return reactors[Math.floorMod(connectionIndex, selectorCount)];
    }

    /**
     * Ultra-fast zero-branch, zero-instanceof outbound message submission.
     * Delegates directly to polymorphic connection virtual call.
     */
    public void submitOutboundMessage(
            String connectionId,
            Object message
    ) {
        Connection connection = connectionRegistry.getConnection(connectionId);
        if (connection == null) {
            return;
        }

        byte[] payload = outboundMessageHandler.onSend(connectionId, message);

        try {
            connection.sendOutboundPayload(payload);
        } catch (IOException e) {
            log.debug("Outbound transmission failed for connection {}: {}", connectionId, e.getMessage());
        }
    }

    // =========================================================================
    // Independent Outbound Reactor (1 Thread per Reactor)
    // =========================================================================

    public class OutboundReactor {
        @Getter
        private final int index;
        private Selector selector;
        private Thread selectorThread;
        private volatile boolean running = true;

        public OutboundReactor(int index) {
            this.index = index;
        }

        public void initialize() throws IOException {
            this.selector = Selector.open();
        }

        public void start() {
            String threadName = (selectorCount == 1)
                    ? "outbound-selector"
                    : "outbound-selector-" + index;
            selectorThread = new Thread(this::eventLoop, threadName);
            selectorThread.start();
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

        public void wakeupSelector() {
            selector.wakeup();
        }

        private void eventLoop() {
            int idleCycles = 0;
            while (running) {
                try {
                    int readyCount = selector.selectNow();
                    if (readyCount == 0) {
                        if (idleCycles < 1000) {
                            // Sockets might be temporarily full during a drain; sleep 1ms to give kernel buffers time
                            idleCycles++;
                            readyCount = selector.select(1);
                        } else {
                            // Truly idle (no messages): sleep up to 500ms or until woken up by next broadcast
                            readyCount = selector.select(500);
                        }
                        if (readyCount == 0) continue;
                    }

                    // Reset idle counter when traffic is processed
                    idleCycles = 0;

                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                    while (keyIterator.hasNext()) {
                        SelectionKey key = keyIterator.next();
                        keyIterator.remove();

                        if (!key.isValid()) continue;

                        if (key.isWritable()) {
                            writeData(key);
                        }
                    }
                } catch (Exception e) {
                    if (!running) break;
                }
            }
        }

        private void writeData(SelectionKey key) {
            TCPConnection connection = (TCPConnection) key.attachment();
            if (connection == null) return;

            try {
                connection.safeWrite();
            } catch (IOException e) {
                log.debug("Outbound write failed for connection {}: {}", connection.getConnectionId(), e.getMessage());
                try {
                    connectionRegistry.removeConnection(connection.getConnectionId());
                    key.cancel();
                    connection.close();
                } catch (IOException ignored) {
                }
                return;
            }

            // High-performance write interest termination:
            // When the queue is fully drained, disable OP_WRITE immediately to prevent 100% CPU spin.
            // Uses a lock-free double-check to eliminate lost-write race windows if a worker thread
            // enqueued a message concurrently right before or during interestOpsAnd().
            if (key.isValid() && connection.isWriteOver()) {
                try {
                    key.interestOpsAnd(~SelectionKey.OP_WRITE);
                    if (!connection.isWriteOver() && key.isValid()) {
                        key.interestOpsOr(SelectionKey.OP_WRITE);
                    }
                } catch (CancelledKeyException ignored) {
                }
            }
        }

        /**
         * Pre-registers a TCP connection with this outbound reactor at connection establishment time.
         * Interest is initialized to 0 (idle). Returns the created SelectionKey for zero-lookup caching.
         */
        public SelectionKey registerConnection(TCPConnection connection) throws ClosedChannelException {
            SelectableChannel channel = connection.getChannel();
            if (channel != null && channel.isOpen()) {
                selector.wakeup();
                return channel.register(selector, 0, connection);
            }
            return null;
        }

        /**
         * Ultra-fast zero-lock write enablement:
         * Uses the cached SelectionKey and atomic bitwise interestOpsOr without calling channel.keyFor().
         * Avoids redundant wakeup calls if OP_WRITE is already active on the key.
         */
        public void enableWrite(TCPConnection connection) throws ClosedChannelException {
            SelectionKey key = connection.getOutboundKey();
            if (key != null && key.isValid()) {
                try {
                    int currentOps = key.interestOps();
                    if ((currentOps & SelectionKey.OP_WRITE) == 0) {
                        key.interestOpsOr(SelectionKey.OP_WRITE);
                        selector.wakeup();
                    }
                } catch (CancelledKeyException ignored) {
                }
            } else {
                SelectableChannel channel = connection.getChannel();
                if (channel != null && channel.isOpen()) {
                    SelectionKey registered = channel.register(selector, SelectionKey.OP_WRITE, connection);
                    connection.setOutboundKey(registered);
                    selector.wakeup();
                }
            }
        }
    }
}

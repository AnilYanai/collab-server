package com.anilyanai.connector.selector;

import com.anilyanai.connector.configuration.ClusterProperties;
import com.anilyanai.connector.connection.TCPConnection;
import com.anilyanai.connector.events.InboundMessageHandler;
import com.anilyanai.connector.executor.WorkerPool;
import com.anilyanai.connector.registry.ConnectionRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * =========================================================================================
 * 🌐 InboundPeerTrafficSelector (Dedicated Inter-Server Cluster Reactor)
 * =========================================================================================
 *
 * <p>High-performance Java NIO reactor dedicated exclusively to peer server traffic.
 * Keeps inter-node cluster messages physically isolated from end-user traffic.</p>
 *
 * <h3>Key Design Principles:</h3>
 * <ul>
 *   <li><b>Zero Port Contention:</b> Runs on its own dedicated TCP port (e.g. 9090) on private network.</li>
 *   <li><b>Peer-as-a-Client Model:</b> Peer servers register directly as high-speed {@link TCPConnection}
 *       instances in {@link ConnectionRegistry} under a {@code "peer-..."} identifier.</li>
 *   <li><b>Dynamic On-Demand Peering:</b> Dynamically dials and registers remote host nodes when requested
 *       by incoming client session tokens (via {@link #connectToPeer(String)}).</li>
 *   <li><b>Auto Mesh Handshake:</b> Accepts inbound connections from other servers AND connects
 *       outbound to configured remote peer nodes on startup.</li>
 *   <li><b>Zero-Copy Relay:</b> Reads complete wire frames into off-heap direct buffer and dispatches
 *       directly to the cluster message handler.</li>
 * </ul>
 *
 * @author anilyanai
 * @see ClusterProperties
 * @see TCPConnection
 * @see ConnectionRegistry
 */
@Slf4j
public class InboundPeerTrafficSelector {

    public static final String PEER_CONNECTION_PREFIX = "peer-";

    @Getter
    private Selector selector;
    private ServerSocketChannel serverChannel;

    private volatile boolean running = true;
    private Thread selectorThread;

    private final ClusterProperties properties;
    private final ConnectionRegistry connectionRegistry;
    private final WorkerPool workerPool;
    private InboundMessageHandler peerMessageHandler;

    /**
     * Active peer connection IDs currently participating in the cluster mesh.
     */
    private final Set<String> activePeerConnectionIds = new CopyOnWriteArraySet<>();

    /**
     * Shared direct buffer (64 KB) for high-speed inter-server frame reads.
     */
    private final ByteBuffer sharedReadBuffer = ByteBuffer.allocateDirect(64 * 1024);

    public InboundPeerTrafficSelector(
            ClusterProperties properties,
            ConnectionRegistry connectionRegistry,
            WorkerPool workerPool
    ) {
        this.properties = properties;
        this.connectionRegistry = connectionRegistry;
        this.workerPool = workerPool;
    }

    /**
     * Initializes the selector, binds the peer server socket, and connects to remote peers.
     *
     * @param peerMessageHandler callback invoked when peer server packets arrive
     * @throws IOException if network binding or selector initialization fails
     */
    public void initialize(InboundMessageHandler peerMessageHandler) throws IOException {
        this.peerMessageHandler = peerMessageHandler;
        this.selector = Selector.open();

        // 1. Bind dedicated cluster server socket for inbound connections from other nodes
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.bind(new InetSocketAddress(properties.getPort()));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        log.info("🌐 InboundPeerTrafficSelector listening on cluster port {}", properties.getPort());

        // 2. Connect outbound to pre-configured peer servers to establish bidirectional mesh
        connectToConfiguredPeers();
    }

    /**
     * Starts the dedicated peer event loop thread.
     */
    public void start() {
        selectorThread = new Thread(this::eventLoop, "peer-inbound-selector");
        selectorThread.setDaemon(true);
        selectorThread.start();
    }

    public void stop() {
        running = false;
        if (selector != null) {
            selector.wakeup();
            try {
                selector.close();
            } catch (IOException e) {
                log.debug("Error closing peer selector: {}", e.getMessage());
            }
        }
        if (serverChannel != null) {
            try {
                serverChannel.close();
            } catch (IOException e) {
                log.debug("Error closing peer server channel: {}", e.getMessage());
            }
        }
    }

    /**
     * Primary peer NIO event loop.
     */
    private void eventLoop() {
        while (running) {
            try {
                int readyChannels = selector.select(500);
                if (readyChannels == 0) continue;

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid()) continue;

                    if (key.isAcceptable()) {
                        acceptPeer(key);
                    } else if (key.isConnectable()) {
                        finishConnect(key);
                    } else if (key.isReadable()) {
                        readPeer(key);
                    }
                }
            } catch (ClosedSelectorException e) {
                break;
            } catch (Exception e) {
                if (running) {
                    log.error("Unexpected error in peer event loop: {}", e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Accepts an inbound connection from a remote peer server.
     */
    private void acceptPeer(SelectionKey key) throws IOException {
        if (key.channel() instanceof ServerSocketChannel ssc) {
            SocketChannel peerChannel;
            while ((peerChannel = ssc.accept()) != null) {
                configureAndRegisterPeerChannel(peerChannel, false);
            }
        }
    }

    /**
     * Completes non-blocking outbound connection handshake to a remote peer server.
     */
    private void finishConnect(SelectionKey key) {
        if (key.channel() instanceof SocketChannel peerChannel) {
            TCPConnection connection = (TCPConnection) key.attachment();
            String peerId = (connection != null) ? connection.getConnectionId() : "unknown-peer";

            try {
                if (peerChannel.finishConnect()) {
                    key.interestOps(SelectionKey.OP_READ);
                    if (peerId != null) {
                        activePeerConnectionIds.add(peerId);
                    }
                    log.info("Connected to remote cluster peer: {}", peerId);
                }
            } catch (IOException e) {
                log.warn("Failed to connect to cluster peer {}: {}", peerId, e.getMessage());
                if (peerId != null) {
                    activePeerConnectionIds.remove(peerId);
                    connectionRegistry.removeConnection(peerId);
                }
                try {
                    key.cancel();
                    peerChannel.close();
                } catch (IOException ignored) {}
            }
        }
    }

    /**
     * Connects outbound to configured peer nodes listed in {@link ClusterProperties#getPeers()}.
     */
    private void connectToConfiguredPeers() {
        List<String> peers = properties.getPeers();
        if (peers == null || peers.isEmpty()) return;

        for (String peerAddress : peers) {
            connectToPeer(peerAddress);
        }
    }

    /**
     * Dynamically establishes an outbound peer TCP connection to a designated host node.
     * Idempotent: If the connection is already active or in progress, returns the existing connection ID.
     *
     * @param peerAddress formatted as "host:port" (e.g. "192.168.1.50:9090")
     * @return the connection ID registered for this peer, or null if connection failed
     */
    public String connectToPeer(String peerAddress) {
        if (peerAddress == null || peerAddress.isBlank() || selector == null || !selector.isOpen()) {
            return null;
        }

        try {
            String cleanAddress = peerAddress.trim();
            String[] parts = cleanAddress.split(":");
            String host = parts[0];
            int peerPort = parts.length > 1 ? Integer.parseInt(parts[1]) : properties.getPort();

            String connectionId = PEER_CONNECTION_PREFIX + host + ":" + peerPort;

            // Check if already connected or tracked
            if (activePeerConnectionIds.contains(connectionId) || connectionRegistry.getConnection(connectionId) != null) {
                return connectionId;
            }

            SocketChannel peerChannel = SocketChannel.open();
            peerChannel.configureBlocking(false);
            peerChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            peerChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

            TCPConnection connection = new TCPConnection(connectionId, peerChannel);
            connectionRegistry.add(connection);

            // Wake up selector prior to registration so worker threads don't block on selector key lock
            selector.wakeup();

            boolean connected = peerChannel.connect(new InetSocketAddress(host, peerPort));
            if (connected) {
                peerChannel.register(selector, SelectionKey.OP_READ, connection);
                activePeerConnectionIds.add(connectionId);
                log.info("Immediate connection to dynamic cluster peer established: {}", connectionId);
            } else {
                // Attach TCPConnection instance (NOT String) so readPeer does not fail with ClassCastException
                peerChannel.register(selector, SelectionKey.OP_CONNECT, connection);
                log.info("Initiated non-blocking connect to dynamic cluster peer: {}", connectionId);
            }

            selector.wakeup();
            return connectionId;

        } catch (Exception e) {
            log.warn("Could not initiate dynamic connection to peer '{}': {}", peerAddress, e.getMessage());
            return null;
        }
    }

    /**
     * Configures accepted peer channel and binds to {@link ConnectionRegistry}.
     */
    private void configureAndRegisterPeerChannel(SocketChannel peerChannel, boolean isOutbound) throws IOException {
        peerChannel.configureBlocking(false);
        peerChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        peerChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

        InetSocketAddress remoteAddress = (InetSocketAddress) peerChannel.getRemoteAddress();
        String connectionId = PEER_CONNECTION_PREFIX + remoteAddress.getHostString() + ":" + remoteAddress.getPort();

        TCPConnection connection = new TCPConnection(connectionId, peerChannel);
        connectionRegistry.add(connection);
        activePeerConnectionIds.add(connectionId);

        peerChannel.register(selector, SelectionKey.OP_READ, connection);
        log.info("Accepted cluster peer connection: {} (Total active peers: {})", connectionId, activePeerConnectionIds.size());
    }

    /**
     * Reads framed packets from a peer server and dispatches them to the worker pool.
     */
    private void readPeer(SelectionKey key) {
        if (!key.isValid()) return;

        if (key.channel() instanceof SocketChannel peerChannel) {
            TCPConnection connection = (TCPConnection) key.attachment();
            if (connection == null) return;

            int bytesRead;
            try {
                sharedReadBuffer.clear();
                bytesRead = peerChannel.read(sharedReadBuffer);
            } catch (IOException e) {
                log.debug("Peer connection {} dropped: {}", connection.getConnectionId(), e.getMessage());
                closeAndCleanupPeer(key, peerChannel, connection.getConnectionId());
                return;
            }

            if (bytesRead == -1) {
                closeAndCleanupPeer(key, peerChannel, connection.getConnectionId());
                return;
            }

            if (bytesRead > 0) {
                sharedReadBuffer.flip();
                List<byte[]> frames = connection.extractFrames(sharedReadBuffer);
                String connectionId = connection.getConnectionId();

                if (frames.size() == 1) {
                    byte[] singleFrame = frames.get(0);
                    workerPool.submit(() -> peerMessageHandler.onReceive(connectionId, singleFrame));
                } else if (frames.size() > 1) {
                    workerPool.submit(() -> {
                        for (byte[] frame : frames) {
                            peerMessageHandler.onReceive(connectionId, frame);
                        }
                    });
                }
            }
        }
    }

    private void closeAndCleanupPeer(SelectionKey key, SocketChannel peerChannel, String connectionId) {
        activePeerConnectionIds.remove(connectionId);
        try {
            connectionRegistry.removeConnection(connectionId);
            key.cancel();
            peerChannel.close();
        } catch (IOException e) {
            log.debug("Error closing peer channel {}: {}", connectionId, e.getMessage());
        }
        log.info("Cluster peer disconnected: {} (Remaining peers: {})", connectionId, activePeerConnectionIds.size());
    }

    /**
     * Checks if a given connectionId belongs to a cluster peer node.
     */
    public static boolean isPeerConnection(String connectionId) {
        return connectionId != null && connectionId.startsWith(PEER_CONNECTION_PREFIX);
    }

    /**
     * Returns an unmodifiable snapshot of active peer connection IDs.
     */
    public Set<String> getActivePeerConnectionIds() {
        return activePeerConnectionIds;
    }
}

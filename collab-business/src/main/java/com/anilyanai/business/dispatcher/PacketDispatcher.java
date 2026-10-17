package com.anilyanai.business.dispatcher;

import com.anilyanai.business.broadcast.BroadcastService;
import com.anilyanai.business.broadcast.MessagePublisher;
import com.anilyanai.business.peer.PeerRoutingService;
import com.anilyanai.business.security.AdminAuthorizer;
import com.anilyanai.business.security.SessionAuthorizer;
import com.anilyanai.business.state.SessionStateManager;
import com.anilyanai.connector.registry.ConnectionRegistry;
import com.anilyanai.engine.session.SessionRegistry;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.PacketCodec;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * =========================================================================================
 * 🚀 PacketDispatcher (Business Packet Routing & State Orchestrator)
 * =========================================================================================
 *
 * <p>Central coordinator orchestrating binary protocol decoding, cryptographic token
 * authentication, session membership management, and low-latency packet fan-out.</p>
 *
 * <h3>Architectural Design & Isolated Inbound Pipelines:</h3>
 * <ul>
 *   <li><b>Separation of Concerns:</b> Inbound routing logic is partitioned into dedicated classes:
 *       <ul>
 *         <li>{@link ClientInboundRouter} - Handles all normal end-user client routes (JOIN, LEAVE, BROADCAST, DIRECT, GET_STATE, CLOSE_SESSION).</li>
 *         <li>{@link PeerInboundRouter} - Handles inter-server cluster mesh traffic (PEER JOIN, PEER LEAVE, PEER BROADCAST, PEER CLOSE).</li>
 *       </ul>
 *   </li>
 *   <li><b>Zero Condition Overhead:</b> Inbound packets are routed directly to their isolated handler pipelines
 *       without added intermediate conditional checks, preserving maximum throughput.</li>
 *   <li><b>Fast Zero-GC Broadcast Path:</b> Direct opcode header inspection via fast-header peekers to bypass
 *       full payload decoding and object allocation.</li>
 *   <li><b>Single Session Fetch & Connection Caching:</b> Minimizes hash lookups during high-frequency fan-out.</li>
 * </ul>
 *
 * @author anilyanai
 */
@Slf4j
@Getter
public class PacketDispatcher {

    private final SessionRegistry sessionRegistry;
    private final PacketCodec packetCodec;
    private final SessionStateManager sessionStateManager;
    private final ClientInboundRouter clientInboundRouter;
    private final PeerInboundRouter peerInboundRouter;

    public PacketDispatcher(
            SessionRegistry sessionRegistry,
            MessagePublisher messagePublisher,
            PacketCodec packetCodec,
            SessionAuthorizer sessionAuthorizer,
            AdminAuthorizer adminAuthorizer,
            BroadcastService broadcastService,
            SessionStateManager sessionStateManager,
            PeerRoutingService peerRoutingService
    ) {
        this(sessionRegistry, messagePublisher, packetCodec, sessionAuthorizer, adminAuthorizer,
                broadcastService, sessionStateManager, peerRoutingService, null);
    }

    public PacketDispatcher(
            SessionRegistry sessionRegistry,
            MessagePublisher messagePublisher,
            PacketCodec packetCodec,
            SessionAuthorizer sessionAuthorizer,
            AdminAuthorizer adminAuthorizer,
            BroadcastService broadcastService,
            SessionStateManager sessionStateManager,
            PeerRoutingService peerRoutingService,
            ConnectionRegistry connectionRegistry
    ) {
        this.sessionRegistry = sessionRegistry;
        this.packetCodec = packetCodec;
        this.sessionStateManager = sessionStateManager;
        this.clientInboundRouter = new ClientInboundRouter(
                sessionRegistry,
                messagePublisher,
                packetCodec,
                sessionAuthorizer,
                adminAuthorizer,
                broadcastService,
                peerRoutingService,
                connectionRegistry
        );
        this.peerInboundRouter = new PeerInboundRouter(
                sessionRegistry,
                messagePublisher,
                packetCodec,
                broadcastService
        );
    }

    public PacketDispatcher(
            SessionRegistry sessionRegistry,
            ClientInboundRouter clientInboundRouter,
            PeerInboundRouter peerInboundRouter,
            PacketCodec packetCodec,
            SessionStateManager sessionStateManager
    ) {
        this.sessionRegistry = sessionRegistry;
        this.clientInboundRouter = clientInboundRouter;
        this.peerInboundRouter = peerInboundRouter;
        this.packetCodec = packetCodec;
        this.sessionStateManager = sessionStateManager;
    }

    /**
     * Entry point for messages arriving from end-user client connections (TCP or UDP).
     * Routes directly to {@link ClientInboundRouter#onReceive(String, byte[])}.
     */
    public void onReceive(String connectionId, byte[] rawData) {
        clientInboundRouter.onReceive(connectionId, rawData);
    }

    /**
     * Entry point for messages arriving from peer cluster nodes over dedicated cluster port.
     * Routes directly to {@link PeerInboundRouter#onPeerReceive(String, byte[])}.
     */
    public void onPeerReceive(String peerConnectionId, byte[] rawData) {
        peerInboundRouter.onPeerReceive(peerConnectionId, rawData);
    }

    /**
     * Serializes an outbound object or packet into wire bytes.
     */
    public byte[] onSend(String connectionId, Object message) {
        if (message instanceof byte[] bytes) {
            return bytes;
        }
        if (message instanceof Packet packet) {
            return packetCodec.encode(packet);
        }
        throw new IllegalArgumentException("Unsupported outbound message type: " +
                (message == null ? "null" : message.getClass().getName()));
    }

    public void sendAck(String connectionId, String sessionId) {
        clientInboundRouter.sendAck(connectionId, sessionId);
    }

    public void sendError(String connectionId, String sessionId, String errorMessage) {
        clientInboundRouter.sendError(connectionId, sessionId, errorMessage);
    }
}

package com.anilyanai.business.dispatcher;

import com.anilyanai.business.broadcast.BroadcastService;
import com.anilyanai.business.broadcast.MessagePublisher;
import com.anilyanai.business.peer.PeerRoutingService;
import com.anilyanai.business.security.AdminAuthorizer;
import com.anilyanai.business.security.AuthResult;
import com.anilyanai.business.security.SessionAuthorizer;
import com.anilyanai.connector.connection.Connection;
import com.anilyanai.connector.registry.ConnectionRegistry;
import com.anilyanai.engine.session.Session;
import com.anilyanai.engine.session.SessionRegistry;
import com.anilyanai.protocol.Opcode;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.BinaryPacketCodec;
import com.anilyanai.protocol.codec.PacketCodec;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * =========================================================================================
 * ⚡ ClientInboundRouter (Normal Users / End-User Client Inbound Pipeline)
 * =========================================================================================
 *
 * <p>Dedicated low-latency inbound route for client messages arriving over TCP or UDP.</p>
 *
 * <h3>Key Optimizations & Characteristics:</h3>
 * <ul>
 *   <li><b>Fast Zero-GC Broadcast Path:</b> Uses {@link BinaryPacketCodec#peekFastHeader(byte[])}
 *       to inspect opcode and sessionId directly from raw wire bytes, bypassing packet decoding
 *       and memory allocations.</li>
 *   <li><b>Single Session Fetch:</b> Resolves {@link Session} once and passes it directly to state
 *       saving and broadcasting, avoiding repeated session registry map lookups.</li>
 *   <li><b>Direct Connection Caching:</b> Caches live {@link Connection} instances into sessions
 *       to eliminate hash table lookups during fan-out.</li>
 *   <li><b>Zero Condition State Saving:</b> Delegates session state persistence directly to
 *       {@link Session#saveState(byte[])}.</li>
 * </ul>
 *
 * @author anilyanai
 */
@Slf4j
public class ClientInboundRouter {

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final SessionRegistry sessionRegistry;
    private final MessagePublisher messagePublisher;
    private final PacketCodec packetCodec;
    private final SessionAuthorizer sessionAuthorizer;
    private final AdminAuthorizer adminAuthorizer;
    private final BroadcastService broadcastService;
    private final PeerRoutingService peerRoutingService;
    private final ConnectionRegistry connectionRegistry;

    public ClientInboundRouter(
            SessionRegistry sessionRegistry,
            MessagePublisher messagePublisher,
            PacketCodec packetCodec,
            SessionAuthorizer sessionAuthorizer,
            AdminAuthorizer adminAuthorizer,
            BroadcastService broadcastService,
            PeerRoutingService peerRoutingService,
            ConnectionRegistry connectionRegistry
    ) {
        this.sessionRegistry = sessionRegistry;
        this.messagePublisher = messagePublisher;
        this.packetCodec = packetCodec;
        this.sessionAuthorizer = sessionAuthorizer;
        this.adminAuthorizer = adminAuthorizer;
        this.broadcastService = broadcastService;
        this.peerRoutingService = peerRoutingService;
        this.connectionRegistry = connectionRegistry;
    }

    /**
     * Handles inbound messages from normal client connections.
     */
    public void onReceive(String connectionId, byte[] rawData) {
        if (connectionId == null || rawData == null || rawData.length == 0) {
            return;
        }

        // Fast zero-allocation path for broadcast packets:
        // Extracts opcode and sessionId directly from raw wire header without allocating payload arrays or Packet records!
        BinaryPacketCodec.FastHeader fastHeader = BinaryPacketCodec.peekFastHeader(rawData);
        if (fastHeader != null) {
            if (fastHeader.opcode() == Opcode.BROADCAST.getCode()) {
                handleFastClientBroadcast(connectionId, fastHeader.sessionId(), rawData);
                return;
            } else if (fastHeader.opcode() == Opcode.BROADCAST_ALL.getCode()) {
                handleFastClientBroadcastAll(fastHeader.sessionId(), rawData);
                return;
            }
        }

        Packet packet;
        try {
            packet = packetCodec.decode(rawData);
        } catch (Exception e) {
            log.warn("Failed to decode packet from connection {}: {}", connectionId, e.getMessage());
            sendError(connectionId, null, "Malformed packet: " + e.getMessage());
            return;
        }

        dispatchClientPacket(connectionId, packet, rawData);
    }

    private void dispatchClientPacket(String connectionId, Packet packet, byte[] rawBytes) {
        switch (packet.opcode()) {
            case JOIN_SESSION -> handleClientJoin(connectionId, packet);
            case LEAVE_SESSION -> handleClientLeave(connectionId, packet);
            case BROADCAST -> handleClientBroadcast(connectionId, packet, rawBytes);
            case BROADCAST_ALL -> handleClientBroadcastAll(connectionId, packet, rawBytes);
            case DIRECT -> handleDirect(connectionId, packet, rawBytes);
            case GET_STATE -> handleGetState(connectionId, packet);
            case CLOSE_SESSION -> handleCloseSession(connectionId, packet);
            default -> {
                log.warn("Unhandled opcode {} from client {}", packet.opcode(), connectionId);
                sendError(connectionId, packet.sessionId(), "Unsupported opcode: " + packet.opcode());
            }
        }
    }

    private void handleClientJoin(String connectionId, Packet packet) {
        String sessionId = packet.sessionId();
        if (sessionId == null || sessionId.isBlank()) {
            sendError(connectionId, null, "Session ID cannot be empty in JOIN_SESSION");
            return;
        }

        AuthResult authResult = sessionAuthorizer.authorizeWithResult(sessionId, connectionId, packet.payload());
        if (!authResult.authorized()) {
            log.warn("Client {} failed authorization for session {}", connectionId, sessionId);
            sendError(connectionId, sessionId, "Unauthorized: Invalid or expired session credentials");
            return;
        }

        // Dynamic inter-server cluster peering
        if (authResult.hostServer() != null && peerRoutingService != null) {
            String peerHost = authResult.hostServer();
            String peerId = peerRoutingService.ensurePeerConnected(peerHost);
            if (peerId != null) {
                sessionRegistry.joinPeer(sessionId, peerId);
                log.info("🌐 Dynamic peer connection activated to session anchor {} ({}) for session {}",
                        peerHost, peerId, sessionId);
            }
        }

        boolean stateful = packet.isStatePreserved();

        // Direct Connection caching: caches Connection object in Session to eliminate hash lookups
        Connection connection = (connectionRegistry != null) ? connectionRegistry.getConnection(connectionId) : null;
        if (connection != null) {
            sessionRegistry.join(sessionId, connection, stateful);
        } else {
            sessionRegistry.join(sessionId, connectionId, stateful);
        }

        sendAck(connectionId, sessionId);

        // Stream replay state if available
        List<byte[]> history = sessionRegistry.getSessionHistory(sessionId);
        if (!history.isEmpty()) {
            for (byte[] frame : history) {
                messagePublisher.publish(connectionId, frame);
            }
        } else {
            sendPacket(connectionId, Packet.ofGetState(sessionId));
        }

        log.debug("Client {} joined session {} (stateful={})", connectionId, sessionId, stateful);
    }

    private void handleClientLeave(String connectionId, Packet packet) {
        String sessionId = packet.sessionId();
        if (sessionId != null) {
            sessionRegistry.leave(sessionId, connectionId);
            sendAck(connectionId, sessionId);
            log.debug("Client {} left session {}", connectionId, sessionId);
        }
    }

    private void handleFastClientBroadcast(String connectionId, String sessionId, byte[] rawBytes) {
        if (sessionId == null || sessionId.isBlank()) return;
        Session session = sessionRegistry.getSession(sessionId);
        if (session != null) {
            session.saveState(rawBytes);
            broadcastService.broadcastFromClient(connectionId, session, rawBytes);
        } else {
            sessionRegistry.saveSessionState(sessionId, rawBytes);
            broadcastService.broadcastFromClient(connectionId, sessionId, rawBytes);
        }
    }

    private void handleFastClientBroadcastAll(String sessionId, byte[] rawBytes) {
        if (sessionId == null || sessionId.isBlank()) return;
        Session session = sessionRegistry.getSession(sessionId);
        if (session != null) {
            session.saveState(rawBytes);
            broadcastService.broadcastAll(session, rawBytes);
        } else {
            sessionRegistry.saveSessionState(sessionId, rawBytes);
            broadcastService.broadcastAll(sessionId, rawBytes);
        }
    }

    private void handleClientBroadcast(String connectionId, Packet packet, byte[] rawBytes) {
        String sessionId = packet.sessionId();
        if (sessionId == null) return;

        Session session = sessionRegistry.getSession(sessionId);
        if (session != null) {
            session.saveState(rawBytes);
            broadcastService.broadcastFromClient(connectionId, session, rawBytes);
        } else {
            sessionRegistry.saveSessionState(sessionId, rawBytes);
            broadcastService.broadcastFromClient(connectionId, sessionId, rawBytes);
        }
    }

    private void handleClientBroadcastAll(String connectionId, Packet packet, byte[] rawBytes) {
        String sessionId = packet.sessionId();
        if (sessionId == null) return;

        Session session = sessionRegistry.getSession(sessionId);
        if (session != null) {
            session.saveState(rawBytes);
            broadcastService.broadcastAll(session, rawBytes);
        } else {
            sessionRegistry.saveSessionState(sessionId, rawBytes);
            broadcastService.broadcastAll(sessionId, rawBytes);
        }
    }

    private void handleDirect(String connectionId, Packet packet, byte[] rawBytes) {
        String targetId = packet.targetId();
        if (targetId == null || targetId.isBlank()) {
            sendError(connectionId, packet.sessionId(), "Target connection ID cannot be empty in DIRECT");
            return;
        }

        broadcastService.sendDirect(targetId, rawBytes);
    }

    private void handleGetState(String connectionId, Packet packet) {
        String sessionId = packet.sessionId();
        if (sessionId == null) return;

        List<byte[]> history = sessionRegistry.getSessionHistory(sessionId);
        if (!history.isEmpty()) {
            for (byte[] frame : history) {
                messagePublisher.publish(connectionId, frame);
            }
        } else {
            sendPacket(connectionId, Packet.ofGetState(sessionId));
        }
    }

    private void handleCloseSession(String connectionId, Packet packet) {
        String sessionId = packet.sessionId();
        if (sessionId == null) {
            sendError(connectionId, null, "Session ID cannot be empty in CLOSE_SESSION");
            return;
        }

        if (!adminAuthorizer.authorizeAdmin(sessionId, connectionId, packet.payload())) {
            log.warn("Unauthorized CLOSE_SESSION attempt on room {} by connection {}", sessionId, connectionId);
            sendError(connectionId, sessionId, "Unauthorized: Master Admin privileges required to close session");
            return;
        }

        Packet notifyClose = new Packet(
                Packet.CURRENT_VERSION,
                Opcode.CLOSE_SESSION,
                Packet.FLAG_NONE,
                0,
                (short) 0,
                (short) 0,
                sessionId,
                null,
                EMPTY_PAYLOAD
        );
        byte[] closeBytes = packetCodec.encode(notifyClose);

        Set<String> evictedMembers = sessionRegistry.closeSession(sessionId);
        for (String memberId : evictedMembers) {
            if (!memberId.equals(connectionId)) {
                messagePublisher.publish(memberId, closeBytes);
            }
        }

        sendAck(connectionId, sessionId);
        log.info("Session {} closed by admin {}. Evicted {} members.", sessionId, connectionId, evictedMembers.size());
    }

    public void sendAck(String connectionId, String sessionId) {
        Packet ack = new Packet(
                Packet.CURRENT_VERSION,
                Opcode.ACK,
                Packet.FLAG_NONE,
                0,
                (short) 0,
                (short) 1,
                sessionId,
                null,
                EMPTY_PAYLOAD
        );
        sendPacket(connectionId, ack);
    }

    public void sendError(String connectionId, String sessionId, String errorMessage) {
        byte[] payload = (errorMessage != null)
                ? errorMessage.getBytes(StandardCharsets.UTF_8)
                : EMPTY_PAYLOAD;
        Packet error = new Packet(
                Packet.CURRENT_VERSION,
                Opcode.ERROR,
                Packet.FLAG_NONE,
                0,
                (short) 0,
                (short) 1,
                sessionId,
                null,
                payload
        );
        sendPacket(connectionId, error);
    }

    private void sendPacket(String connectionId, Packet packet) {
        byte[] encoded = packetCodec.encode(packet);
        messagePublisher.publish(connectionId, encoded);
    }
}

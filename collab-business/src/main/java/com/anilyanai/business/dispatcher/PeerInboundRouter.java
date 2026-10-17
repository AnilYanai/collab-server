package com.anilyanai.business.dispatcher;

import com.anilyanai.business.broadcast.BroadcastService;
import com.anilyanai.business.broadcast.MessagePublisher;
import com.anilyanai.engine.session.Session;
import com.anilyanai.engine.session.SessionRegistry;
import com.anilyanai.protocol.Opcode;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.BinaryPacketCodec;
import com.anilyanai.protocol.codec.PacketCodec;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;

/**
 * =========================================================================================
 * 🌐 PeerInboundRouter (Inter-Server Cluster Mesh Inbound Pipeline)
 * =========================================================================================
 *
 * <p>Dedicated low-latency inbound route for messages arriving from peer cluster nodes
 * over dedicated cluster connections.</p>
 *
 * <h3>Key Optimizations & Characteristics:</h3>
 * <ul>
 *   <li><b>Fast Zero-GC Peer Broadcast Path:</b> Direct opcode header inspection via
 *       {@link BinaryPacketCodec#peekFastHeader(byte[])} to forward inter-server broadcasts without
 *       decoding packets into heap objects.</li>
 *   <li><b>Direct Session Caching:</b> Fetches {@link Session} once for state persistence and
 *       peer fan-out, avoiding repeated session registry map lookups.</li>
 *   <li><b>Cluster Loop Prevention:</b> Peer broadcasts are only routed to local session clients,
 *       preventing echo cycles across cluster peers.</li>
 * </ul>
 *
 * @author anilyanai
 */
@Slf4j
public class PeerInboundRouter {

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final SessionRegistry sessionRegistry;
    private final MessagePublisher messagePublisher;
    private final PacketCodec packetCodec;
    private final BroadcastService broadcastService;

    public PeerInboundRouter(
            SessionRegistry sessionRegistry,
            MessagePublisher messagePublisher,
            PacketCodec packetCodec,
            BroadcastService broadcastService
    ) {
        this.sessionRegistry = sessionRegistry;
        this.messagePublisher = messagePublisher;
        this.packetCodec = packetCodec;
        this.broadcastService = broadcastService;
    }

    /**
     * Handles inbound messages from peer cluster nodes.
     */
    public void onPeerReceive(String peerConnectionId, byte[] rawData) {
        if (peerConnectionId == null || rawData == null || rawData.length == 0) {
            return;
        }

        BinaryPacketCodec.FastHeader fastHeader = BinaryPacketCodec.peekFastHeader(rawData);
        if (fastHeader != null) {
            if (fastHeader.opcode() == Opcode.BROADCAST.getCode() || fastHeader.opcode() == Opcode.BROADCAST_ALL.getCode()) {
                if (fastHeader.sessionId() != null && !fastHeader.sessionId().isBlank()) {
                    Session session = sessionRegistry.getSession(fastHeader.sessionId());
                    if (session != null) {
                        session.saveState(rawData);
                        broadcastService.broadcastFromPeer(peerConnectionId, session, rawData);
                    } else {
                        sessionRegistry.saveSessionState(fastHeader.sessionId(), rawData);
                        broadcastService.broadcastFromPeer(peerConnectionId, fastHeader.sessionId(), rawData);
                    }
                    return;
                }
            }
        }

        Packet packet;
        try {
            packet = packetCodec.decode(rawData);
        } catch (Exception e) {
            log.warn("Failed to decode packet from peer {}: {}", peerConnectionId, e.getMessage());
            return;
        }

        dispatchPeerPacket(peerConnectionId, packet, rawData);
    }

    private void dispatchPeerPacket(String peerConnectionId, Packet packet, byte[] rawBytes) {
        switch (packet.opcode()) {
            case JOIN_SESSION -> handlePeerJoin(peerConnectionId, packet);
            case LEAVE_SESSION -> handlePeerLeave(peerConnectionId, packet);
            case BROADCAST -> handlePeerBroadcast(peerConnectionId, packet, rawBytes);
            case BROADCAST_ALL -> handlePeerBroadcastAll(peerConnectionId, packet, rawBytes);
            case DIRECT -> handleDirect(peerConnectionId, packet, rawBytes);
            case CLOSE_SESSION -> handlePeerCloseSession(peerConnectionId, packet, rawBytes);
            default -> log.debug("Ignored unhandled peer opcode {} from {}", packet.opcode(), peerConnectionId);
        }
    }

    private void handlePeerJoin(String peerConnectionId, Packet packet) {
        String sessionId = packet.sessionId();
        if (sessionId == null || sessionId.isBlank()) return;

        sessionRegistry.joinPeer(sessionId, peerConnectionId);
        sendAck(peerConnectionId, sessionId);

        List<byte[]> history = sessionRegistry.getSessionHistory(sessionId);
        for (byte[] frame : history) {
            messagePublisher.publish(peerConnectionId, frame);
        }
        log.info("Cluster peer {} registered into session {}", peerConnectionId, sessionId);
    }

    private void handlePeerLeave(String peerConnectionId, Packet packet) {
        String sessionId = packet.sessionId();
        if (sessionId != null) {
            sessionRegistry.leave(sessionId, peerConnectionId);
            sendAck(peerConnectionId, sessionId);
        }
    }

    private void handlePeerBroadcast(String peerConnectionId, Packet packet, byte[] rawBytes) {
        String sessionId = packet.sessionId();
        if (sessionId == null) return;

        Session session = sessionRegistry.getSession(sessionId);
        if (session != null) {
            session.saveState(rawBytes);
            broadcastService.broadcastFromPeer(peerConnectionId, session, rawBytes);
        } else {
            sessionRegistry.saveSessionState(sessionId, rawBytes);
            broadcastService.broadcastFromPeer(peerConnectionId, sessionId, rawBytes);
        }
    }

    private void handlePeerBroadcastAll(String peerConnectionId, Packet packet, byte[] rawBytes) {
        String sessionId = packet.sessionId();
        if (sessionId == null) return;

        Session session = sessionRegistry.getSession(sessionId);
        if (session != null) {
            session.saveState(rawBytes);
            broadcastService.broadcastFromPeer(peerConnectionId, session, rawBytes);
        } else {
            sessionRegistry.saveSessionState(sessionId, rawBytes);
            broadcastService.broadcastFromPeer(peerConnectionId, sessionId, rawBytes);
        }
    }

    private void handleDirect(String peerConnectionId, Packet packet, byte[] rawBytes) {
        String targetId = packet.targetId();
        if (targetId == null || targetId.isBlank()) {
            return;
        }

        broadcastService.sendDirect(targetId, rawBytes);
    }

    private void handlePeerCloseSession(String peerConnectionId, Packet packet, byte[] rawBytes) {
        String sessionId = packet.sessionId();
        if (sessionId == null) return;

        Set<String> evictedMembers = sessionRegistry.closeSession(sessionId);
        for (String memberId : evictedMembers) {
            messagePublisher.publish(memberId, rawBytes);
        }
        log.info("Session {} dismantled on cluster signal from peer {}", sessionId, peerConnectionId);
    }

    public void sendAck(String peerConnectionId, String sessionId) {
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
        sendPacket(peerConnectionId, ack);
    }

    private void sendPacket(String connectionId, Packet packet) {
        byte[] encoded = packetCodec.encode(packet);
        messagePublisher.publish(connectionId, encoded);
    }
}

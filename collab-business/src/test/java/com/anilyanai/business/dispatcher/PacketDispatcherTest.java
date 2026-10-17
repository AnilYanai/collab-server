package com.anilyanai.business.dispatcher;

import com.anilyanai.business.broadcast.BroadcastService;
import com.anilyanai.business.broadcast.MessagePublisher;
import com.anilyanai.business.peer.PeerRoutingService;
import com.anilyanai.business.security.AdminAuthorizer;
import com.anilyanai.business.security.AuthResult;
import com.anilyanai.business.security.SessionAuthorizer;
import com.anilyanai.business.state.SessionStateManager;
import com.anilyanai.engine.session.Session;
import com.anilyanai.engine.session.SessionRegistry;
import com.anilyanai.protocol.Opcode;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.BinaryPacketCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class PacketDispatcherTest {

    private SessionRegistry sessionRegistry;
    private MessagePublisher messagePublisher;
    private SessionStateManager sessionStateManager;
    private BroadcastService broadcastService;
    private PeerRoutingService peerRoutingService;
    private PacketDispatcher dispatcher;
    private BinaryPacketCodec codec;
    private AtomicReference<String> connectedPeer;

    @BeforeEach
    void setUp() {
        sessionRegistry = mock(SessionRegistry.class);
        messagePublisher = mock(MessagePublisher.class);
        sessionStateManager = mock(SessionStateManager.class);
        connectedPeer = new AtomicReference<>();

        peerRoutingService = new PeerRoutingService(
                () -> Set.of("peer-node-2"),
                host -> {
                    connectedPeer.set(host);
                    return "peer-" + host;
                }
        );

        broadcastService = new BroadcastService(
                sessionRegistry,
                messagePublisher
        );

        dispatcher = new PacketDispatcher(
                sessionRegistry,
                messagePublisher,
                new BinaryPacketCodec(),
                SessionAuthorizer.PERMISSIVE,
                AdminAuthorizer.PERMISSIVE,
                broadcastService,
                sessionStateManager,
                peerRoutingService
        );
        codec = new BinaryPacketCodec();
    }

    @Test
    void testJoinSessionTriggersSessionRegistryAndSendsAckAndEmptyState() {
        Packet joinPacket = Packet.ofJoin("room-1");
        byte[] encoded = codec.encode(joinPacket);

        dispatcher.onReceive("conn-1", encoded);

        verify(sessionRegistry).join("room-1", "conn-1", false);
        ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
        verify(messagePublisher, times(2)).publish(eq("conn-1"), captor.capture());

        List<byte[]> sent = captor.getAllValues();
        Packet p1 = codec.decode(sent.get(0));
        Packet p2 = codec.decode(sent.get(1));
        assertEquals(Opcode.ACK, p1.opcode());
        assertEquals("room-1", p1.sessionId());
        assertEquals(Opcode.GET_STATE, p2.opcode());
        assertEquals("room-1", p2.sessionId());
    }

    @Test
    void testJoinWithDesignatedHostServerTriggersDynamicPeerConnection() {
        SessionAuthorizer customAuthorizer = new SessionAuthorizer() {
            @Override
            public boolean authorize(String sessionId, String connectionId, byte[] authPayload) {
                return true;
            }

            @Override
            public AuthResult authorizeWithResult(String sessionId, String connectionId, byte[] authPayload) {
                return AuthResult.approved("192.168.1.50:9090");
            }
        };

        PacketDispatcher dynamicDispatcher = new PacketDispatcher(
                sessionRegistry,
                messagePublisher,
                new BinaryPacketCodec(),
                customAuthorizer,
                AdminAuthorizer.PERMISSIVE,
                broadcastService,
                sessionStateManager,
                peerRoutingService
        );

        Packet joinPacket = Packet.ofJoin("meeting-42");
        byte[] encoded = codec.encode(joinPacket);

        dynamicDispatcher.onReceive("conn-edge-1", encoded);

        assertEquals("192.168.1.50:9090", connectedPeer.get());
        verify(sessionRegistry).joinPeer("meeting-42", "peer-192.168.1.50:9090");
        verify(sessionRegistry).join("meeting-42", "conn-edge-1", false);
    }

    @Test
    void testJoinStatefulSessionReplaysHistoryToLateComer() {
        byte[] historicalFrame = "historical-code-change".getBytes(StandardCharsets.UTF_8);
        when(sessionRegistry.getSessionHistory("room-stateful")).thenReturn(List.of(historicalFrame));

        Packet joinPacket = Packet.ofJoin("room-stateful", true);
        byte[] encoded = codec.encode(joinPacket);

        dispatcher.onReceive("late-joiner-1", encoded);

        verify(sessionRegistry).join("room-stateful", "late-joiner-1", true);
        verify(messagePublisher, atLeastOnce()).publish(eq("late-joiner-1"), eq(historicalFrame));
    }

    @Test
    void testLeaveSessionTriggersSessionRegistryAndSendsAck() {
        Packet leavePacket = Packet.ofLeave("room-1");
        byte[] encoded = codec.encode(leavePacket);

        dispatcher.onReceive("conn-1", encoded);

        verify(sessionRegistry).leave("room-1", "conn-1");
        ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
        verify(messagePublisher).publish(eq("conn-1"), captor.capture());

        Packet ack = codec.decode(captor.getValue());
        assertEquals(Opcode.ACK, ack.opcode());
    }

    @Test
    void testLocalBroadcastRoutesToSessionClientsAndClusterPeers() {
        when(sessionRegistry.getLocalClients("room-1")).thenReturn(Set.of("conn-1", "conn-2", "conn-3"));
        when(sessionRegistry.getPeerServers("room-1")).thenReturn(Set.of("peer-node-2"));

        Packet broadcastPacket = Packet.ofBroadcast("room-1", "hello all".getBytes(StandardCharsets.UTF_8));
        byte[] encoded = codec.encode(broadcastPacket);

        dispatcher.onReceive("conn-1", encoded);

        verify(sessionRegistry).saveSessionState("room-1", encoded);
        verify(messagePublisher).publish(eq("conn-2"), eq(encoded));
        verify(messagePublisher).publish(eq("conn-3"), eq(encoded));
        verify(messagePublisher, never()).publish(eq("conn-1"), any(byte[].class));
        verify(messagePublisher).publish(eq("peer-node-2"), eq(encoded));
    }

    @Test
    void testBroadcastWithSingleSessionFetchEliminatesRedundantLookups() {
        Session session = spy(new Session("room-fast"));
        session.joinLocalClient("conn-1");
        session.joinLocalClient("conn-2");
        session.joinPeerServer("peer-node-2");

        when(sessionRegistry.getSession("room-fast")).thenReturn(session);

        Packet broadcastPacket = Packet.ofBroadcast("room-fast", "fast broadcast".getBytes(StandardCharsets.UTF_8));
        byte[] encoded = codec.encode(broadcastPacket);

        dispatcher.onReceive("conn-1", encoded);

        // Verify getSession was queried exactly ONCE
        verify(sessionRegistry, times(1)).getSession("room-fast");

        // Verify saveState was invoked directly on session with zero subsequent map lookups
        verify(session, times(1)).saveState(encoded);
        verify(sessionRegistry, never()).saveSessionState(eq("room-fast"), any());

        // Verify fan-out occurred properly
        verify(messagePublisher).publish(eq("conn-2"), eq(encoded));
        verify(messagePublisher).publish(eq("peer-node-2"), eq(encoded));
        verify(messagePublisher, never()).publish(eq("conn-1"), any(byte[].class));
    }

    @Test
    void testPeerBroadcastDoesNotRelayBackToOtherPeers() {
        when(sessionRegistry.getLocalClients("room-1")).thenReturn(Set.of("conn-1", "conn-2"));
        when(sessionRegistry.getPeerServers("room-1")).thenReturn(Set.of("peer-node-2"));

        Packet broadcastPacket = Packet.ofBroadcast("room-1", "cluster update".getBytes(StandardCharsets.UTF_8));
        byte[] encoded = codec.encode(broadcastPacket);

        dispatcher.onPeerReceive("peer-node-1", encoded);

        verify(sessionRegistry).saveSessionState("room-1", encoded);
        verify(messagePublisher).publish(eq("conn-1"), eq(encoded));
        verify(messagePublisher).publish(eq("conn-2"), eq(encoded));
        verify(messagePublisher, never()).publish(eq("peer-node-2"), eq(encoded));
    }

    @Test
    void testMalformedPacketSendsError() {
        byte[] invalidData = new byte[20];
        invalidData[0] = 0x00; // Corrupted magic (expected 0xCA)

        dispatcher.onReceive("conn-1", invalidData);

        ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
        verify(messagePublisher).publish(eq("conn-1"), captor.capture());

        Packet error = codec.decode(captor.getValue());
        assertEquals(Opcode.ERROR, error.opcode());
        assertTrue(new String(error.payload(), StandardCharsets.UTF_8).contains("Malformed packet"));
    }
}

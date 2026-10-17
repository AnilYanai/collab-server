package com.anilyanai.business.dispatcher;

import com.anilyanai.business.broadcast.BroadcastService;
import com.anilyanai.business.broadcast.MessagePublisher;
import com.anilyanai.engine.session.Session;
import com.anilyanai.engine.session.SessionRegistry;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.BinaryPacketCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class PeerInboundRouterTest {

    private SessionRegistry sessionRegistry;
    private MessagePublisher messagePublisher;
    private BroadcastService broadcastService;
    private PeerInboundRouter router;
    private BinaryPacketCodec codec;

    @BeforeEach
    void setUp() {
        sessionRegistry = mock(SessionRegistry.class);
        messagePublisher = mock(MessagePublisher.class);

        broadcastService = new BroadcastService(
                sessionRegistry,
                messagePublisher
        );

        router = new PeerInboundRouter(
                sessionRegistry,
                messagePublisher,
                new BinaryPacketCodec(),
                broadcastService
        );
        codec = new BinaryPacketCodec();
    }

    @Test
    void testPeerBroadcastDoesNotRelayBackToOtherPeers() {
        when(sessionRegistry.getLocalClients("room-1")).thenReturn(Set.of("conn-1", "conn-2"));
        when(sessionRegistry.getPeerServers("room-1")).thenReturn(Set.of("peer-node-2"));

        Packet broadcastPacket = Packet.ofBroadcast("room-1", "cluster update".getBytes(StandardCharsets.UTF_8));
        byte[] encoded = codec.encode(broadcastPacket);

        router.onPeerReceive("peer-node-1", encoded);

        verify(sessionRegistry).saveSessionState("room-1", encoded);
        verify(messagePublisher).publish(eq("conn-1"), eq(encoded));
        verify(messagePublisher).publish(eq("conn-2"), eq(encoded));
        verify(messagePublisher, never()).publish(eq("peer-node-2"), eq(encoded));
    }

    @Test
    void testPeerBroadcastWithExistingSessionDirectlySavesState() {
        Session session = spy(new Session("room-peer"));
        session.joinLocalClient("conn-1");
        session.joinPeerServer("peer-node-2");

        when(sessionRegistry.getSession("room-peer")).thenReturn(session);

        Packet broadcastPacket = Packet.ofBroadcast("room-peer", "cluster data".getBytes(StandardCharsets.UTF_8));
        byte[] encoded = codec.encode(broadcastPacket);

        router.onPeerReceive("peer-node-1", encoded);

        verify(sessionRegistry, times(1)).getSession("room-peer");
        verify(session, times(1)).saveState(encoded);
        verify(sessionRegistry, never()).saveSessionState(eq("room-peer"), any());
        verify(messagePublisher).publish(eq("conn-1"), eq(encoded));
        verify(messagePublisher, never()).publish(eq("peer-node-2"), eq(encoded));
    }
}

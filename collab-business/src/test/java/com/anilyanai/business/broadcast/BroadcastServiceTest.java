package com.anilyanai.business.broadcast;

import com.anilyanai.engine.session.Session;
import com.anilyanai.engine.session.SessionRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.mockito.Mockito.*;

class BroadcastServiceTest {

    private SessionRegistry sessionRegistry;
    private MessagePublisher messagePublisher;
    private BroadcastService broadcastService;

    @BeforeEach
    void setUp() {
        sessionRegistry = mock(SessionRegistry.class);
        messagePublisher = mock(MessagePublisher.class);

        broadcastService = new BroadcastService(
                sessionRegistry,
                messagePublisher
        );
    }

    @Test
    void testBroadcastFromClientFansOutToLocalClientsAndSessionPeers() {
        when(sessionRegistry.getLocalClients("room-1")).thenReturn(Set.of("client-1", "client-2", "client-3"));
        when(sessionRegistry.getPeerServers("room-1")).thenReturn(Set.of("peer-server-9"));

        byte[] frame = "broadcast-data".getBytes(StandardCharsets.UTF_8);

        // Client-1 broadcasts to room-1
        broadcastService.broadcastFromClient("client-1", "room-1", frame);

        // Must publish to client-2, client-3, and peer-server-9, but NEVER back to client-1
        verify(messagePublisher).publish("client-2", frame);
        verify(messagePublisher).publish("client-3", frame);
        verify(messagePublisher).publish("peer-server-9", frame);
        verify(messagePublisher, never()).publish("client-1", frame);
    }

    @Test
    void testDirectSessionBroadcastBypassesSessionRegistryLookups() {
        Session session = new Session("room-fast");
        session.joinLocalClient("user-a");
        session.joinLocalClient("user-b");
        session.joinPeerServer("peer-node-1");

        byte[] payload = "single-session-data".getBytes(StandardCharsets.UTF_8);

        // Broadcast directly using Session instance (Single Session fetch)
        broadcastService.broadcastFromClient("user-a", session, payload);

        verify(messagePublisher).publish("user-b", payload);
        verify(messagePublisher).publish("peer-node-1", payload);
        verify(messagePublisher, never()).publish("user-a", payload);

        // Verify zero calls made to sessionRegistry
        verifyNoInteractions(sessionRegistry);
    }

    @Test
    void testBroadcastFromPeerFansOutOnlyToLocalClientsWithoutPeerEcho() {
        when(sessionRegistry.getLocalClients("room-1")).thenReturn(Set.of("client-1", "client-2"));
        when(sessionRegistry.getPeerServers("room-1")).thenReturn(Set.of("peer-server-2"));

        byte[] frame = "cluster-incoming-data".getBytes(StandardCharsets.UTF_8);

        // Frame arrived from peer-server-1
        broadcastService.broadcastFromPeer("peer-server-1", "room-1", frame);

        // Must publish directly to local clients
        verify(messagePublisher).publish("client-1", frame);
        verify(messagePublisher).publish("client-2", frame);

        // Must NEVER echo to peer-server-2 or peer-server-1
        verify(messagePublisher, never()).publish("peer-server-2", frame);
        verify(messagePublisher, never()).publish("peer-server-1", frame);
    }

    @Test
    void testBroadcastAllSendsToEveryone() {
        when(sessionRegistry.getLocalClients("room-1")).thenReturn(Set.of("client-1", "client-2"));
        when(sessionRegistry.getPeerServers("room-1")).thenReturn(Set.of("peer-server-1"));

        byte[] frame = "all-data".getBytes(StandardCharsets.UTF_8);
        broadcastService.broadcastAll("room-1", frame);

        verify(messagePublisher).publish("client-1", frame);
        verify(messagePublisher).publish("client-2", frame);
        verify(messagePublisher).publish("peer-server-1", frame);
    }
}

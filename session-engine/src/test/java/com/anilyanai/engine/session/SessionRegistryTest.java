package com.anilyanai.engine.session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class SessionRegistryTest {

    private SessionRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SessionRegistry();
    }

    @Test
    void testJoinAndGetMembers() {
        registry.join("room-1", "conn-1");
        registry.join("room-1", "conn-2");
        registry.join("room-2", "conn-3");

        Set<String> room1Members = registry.getMembers("room-1");
        assertEquals(2, room1Members.size());
        assertTrue(room1Members.contains("conn-1"));
        assertTrue(room1Members.contains("conn-2"));

        Set<String> room2Members = registry.getMembers("room-2");
        assertEquals(1, room2Members.size());
        assertTrue(room2Members.contains("conn-3"));

        assertEquals(2, registry.getSessionCount());
    }

    @Test
    void testPeerAndLocalClientHaveSeparateRoutingSets() {
        registry.join("room-1", "local-client");
        registry.joinPeer("room-1", "peer-server");

        assertEquals(Set.of("local-client"), registry.getLocalClients("room-1"));
        assertEquals(Set.of("peer-server"), registry.getPeerServers("room-1"));
        assertEquals(Set.of("local-client", "peer-server"), registry.getMembers("room-1"));
    }

    @Test
    void testLeaveSession() {
        registry.join("room-1", "conn-1");
        registry.join("room-1", "conn-2");

        registry.leave("room-1", "conn-1");

        Set<String> members = registry.getMembers("room-1");
        assertEquals(1, members.size());
        assertTrue(members.contains("conn-2"));
        assertFalse(members.contains("conn-1"));

        // Leaving last member removes session entry
        registry.leave("room-1", "conn-2");
        assertTrue(registry.getMembers("room-1").isEmpty());
    }

    @Test
    void testCloseSession() {
        registry.join("room-1", "conn-1");
        registry.join("room-1", "conn-2");
        registry.join("room-2", "conn-1"); // conn-1 is in both rooms

        AtomicBoolean listenerTriggered = new AtomicBoolean(false);
        registry.registerSessionCloseListener(sessionId -> {
            if ("room-1".equals(sessionId)) {
                listenerTriggered.set(true);
            }
        });

        Set<String> evicted = registry.closeSession("room-1");
        assertEquals(2, evicted.size());
        assertTrue(evicted.contains("conn-1"));
        assertTrue(evicted.contains("conn-2"));
        assertTrue(listenerTriggered.get());

        // room-1 is purged completely
        assertTrue(registry.getMembers("room-1").isEmpty());

        // conn-1 is still in room-2
        Set<String> conn1Sessions = registry.getSessions("conn-1");
        assertEquals(1, conn1Sessions.size());
        assertTrue(conn1Sessions.contains("room-2"));

        // conn-2 has no remaining sessions
        assertTrue(registry.getSessions("conn-2").isEmpty());
    }

    @Test
    void testRemoveConnectionCleansAllSessions() {
        registry.join("room-1", "conn-1");
        registry.join("room-2", "conn-1");
        registry.join("room-1", "conn-2");

        registry.removeConnection("conn-1");

        // conn-1 is gone from room-1, but conn-2 remains
        Set<String> room1 = registry.getMembers("room-1");
        assertEquals(1, room1.size());
        assertTrue(room1.contains("conn-2"));

        // room-2 only had conn-1, so it is now empty
        assertTrue(registry.getMembers("room-2").isEmpty());

        // conn-1 has 0 sessions
        assertTrue(registry.getSessions("conn-1").isEmpty());
    }

    @Test
    void testNonExistentSessionReturnsEmptySet() {
        assertTrue(registry.getMembers("non-existent").isEmpty());
        assertTrue(registry.getSessions("non-existent").isEmpty());
    }
}

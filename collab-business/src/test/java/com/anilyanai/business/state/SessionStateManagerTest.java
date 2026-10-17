package com.anilyanai.business.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SessionStateManagerTest {

    private SessionStateManager stateManager;

    @BeforeEach
    void setUp() {
        stateManager = new SessionStateManager();
    }

    @Test
    void testStatefulSessionPreservesHistoryForLateComers() {
        stateManager.registerStateful("room-1");
        assertTrue(stateManager.isSessionStateful("room-1"));

        byte[] frame1 = "line-1-added".getBytes(StandardCharsets.UTF_8);
        byte[] frame2 = "line-2-added".getBytes(StandardCharsets.UTF_8);
        stateManager.recordSessionState("room-1", frame1);
        stateManager.recordSessionState("room-1", frame2);

        List<byte[]> history = stateManager.getSessionHistory("room-1");
        assertEquals(2, history.size());
        assertArrayEquals(frame1, history.get(0));
        assertArrayEquals(frame2, history.get(1));
    }

    @Test
    void testEphemeralSessionKeepsZeroHistory() {
        // Not registered as stateful
        assertFalse(stateManager.isSessionStateful("video-room"));

        byte[] frame = "video-frame-1".getBytes(StandardCharsets.UTF_8);
        stateManager.recordSessionState("video-room", frame);

        List<byte[]> history = stateManager.getSessionHistory("video-room");
        assertTrue(history.isEmpty());
    }

    @Test
    void testClearSessionPurgesBuffer() {
        stateManager.registerStateful("doc-room");
        stateManager.recordSessionState("doc-room", "data".getBytes(StandardCharsets.UTF_8));
        assertEquals(1, stateManager.getSessionHistory("doc-room").size());

        stateManager.clearSession("doc-room");
        assertFalse(stateManager.isSessionStateful("doc-room"));
        assertTrue(stateManager.getSessionHistory("doc-room").isEmpty());
    }
}

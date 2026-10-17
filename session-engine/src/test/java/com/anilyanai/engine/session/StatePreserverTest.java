package com.anilyanai.engine.session;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("StatePreserver Unit Tests")
class StatePreserverTest {

    @Test
    @DisplayName("Ephemeral session returns cached NO_OP singleton with zero allocations")
    void testEphemeralSessionReusesSingleton() {
        StatePreserver preserver1 = StatePreserver.create(false);
        StatePreserver preserver2 = StatePreserver.create(false);
        StatePreserver preserver3 = StatePreserver.create(false, 100);

        assertSame(StatePreserver.NO_OP, preserver1);
        assertSame(preserver1, preserver2);
        assertSame(preserver1, preserver3);
        assertSame(StatePreserver.empty(), preserver1);
    }

    @Test
    @DisplayName("Ephemeral session does not record packets and returns empty history")
    void testEphemeralSessionNoOp() {
        StatePreserver preserver = StatePreserver.create(false);

        preserver.save("payload-1".getBytes(StandardCharsets.UTF_8));
        preserver.save("payload-2".getBytes(StandardCharsets.UTF_8));

        assertTrue(preserver.getHistory().isEmpty());

        // clear() should be a safe no-op
        assertDoesNotThrow(preserver::clear);
        assertTrue(preserver.getHistory().isEmpty());
    }

    @Test
    @DisplayName("Stateful session preserves packets in FIFO order")
    void testStatefulSessionPreservesHistory() {
        StatePreserver preserver = StatePreserver.create(true);

        byte[] frame1 = "frame-1".getBytes(StandardCharsets.UTF_8);
        byte[] frame2 = "frame-2".getBytes(StandardCharsets.UTF_8);

        preserver.save(frame1);
        preserver.save(frame2);

        List<byte[]> history = preserver.getHistory();
        assertEquals(2, history.size());
        assertArrayEquals(frame1, history.get(0));
        assertArrayEquals(frame2, history.get(1));

        preserver.clear();
        assertTrue(preserver.getHistory().isEmpty());
    }

    @Test
    @DisplayName("Factory method respects custom capacity for stateful sessions")
    void testCustomCapacityFactory() {
        int customCapacity = 3;
        StatePreserver preserver = StatePreserver.create(true, customCapacity);

        for (int i = 1; i <= 5; i++) {
            preserver.save(("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }

        List<byte[]> history = preserver.getHistory();
        assertEquals(3, history.size());
        // Oldest frames msg-1 and msg-2 were overwritten
        assertEquals("msg-3", new String(history.get(0), StandardCharsets.UTF_8));
        assertEquals("msg-4", new String(history.get(1), StandardCharsets.UTF_8));
        assertEquals("msg-5", new String(history.get(2), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Defensive copying prevents corruption when caller mutates byte array after save")
    void testDefensiveCopyingOnSave() {
        StatePreserver preserver = StatePreserver.create(true);

        byte[] mutableBuffer = new byte[]{1, 2, 3};
        preserver.save(mutableBuffer);

        // Caller mutates the buffer in-place
        mutableBuffer[0] = 99;
        mutableBuffer[1] = 88;
        mutableBuffer[2] = 77;

        List<byte[]> history = preserver.getHistory();
        assertEquals(1, history.size());
        // Buffer inside preserver should still have original values
        assertArrayEquals(new byte[]{1, 2, 3}, history.get(0));
    }

    @Test
    @DisplayName("Invalid capacity throws IllegalArgumentException")
    void testInvalidCapacityValidation() {
        assertThrows(IllegalArgumentException.class, () -> new StatePreserver(0));
        assertThrows(IllegalArgumentException.class, () -> new StatePreserver(-10));
        assertThrows(IllegalArgumentException.class, () -> StatePreserver.create(true, 0));
        assertThrows(IllegalArgumentException.class, () -> StatePreserver.create(true, -5));
    }
}

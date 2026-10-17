package com.anilyanai.business.state;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * =========================================================================================
 * 💾 SessionStateManager (Business State & Replay Manager)
 * =========================================================================================
 *
 * <p>Decoupled state management for stateful collaboration sessions. Maintains bounded
 * {@link ReplayRingBuffer} instances per room to replay historical state to late joiners.</p>
 *
 * @author anilyanai
 */
@Slf4j
public class SessionStateManager {

    public static final int DEFAULT_BUFFER_CAPACITY = 100;

    private final Map<String, ReplayRingBuffer> sessionBuffers = new ConcurrentHashMap<>();

    /**
     * Registers a session as stateful with default capacity.
     */
    public void registerStateful(String sessionId) {
        registerStateful(sessionId, DEFAULT_BUFFER_CAPACITY);
    }

    /**
     * Registers a session as stateful with custom capacity.
     */
    public void registerStateful(String sessionId, int capacity) {
        if (sessionId == null || sessionId.isBlank()) return;
        sessionBuffers.computeIfAbsent(sessionId, id -> new ReplayRingBuffer(capacity));
    }

    /**
     * Records a broadcast packet into the session's replay ring buffer if stateful.
     */
    public void recordSessionState(String sessionId, byte[] rawBytes) {
        if (sessionId == null || rawBytes == null) return;
        ReplayRingBuffer buffer = sessionBuffers.get(sessionId);
        if (buffer != null) {
            buffer.append(rawBytes);
        }
    }

    /**
     * Retrieves ordered snapshot history for late comers.
     */
    public List<byte[]> getSessionHistory(String sessionId) {
        if (sessionId == null) return Collections.emptyList();
        ReplayRingBuffer buffer = sessionBuffers.get(sessionId);
        return buffer != null ? buffer.snapshot() : Collections.emptyList();
    }

    /**
     * Checks if a session has an active replay buffer.
     */
    public boolean isSessionStateful(String sessionId) {
        return sessionId != null && sessionBuffers.containsKey(sessionId);
    }

    /**
     * Purges replay buffer for a closed or dismantled session.
     */
    public void clearSession(String sessionId) {
        if (sessionId == null) return;
        ReplayRingBuffer removed = sessionBuffers.remove(sessionId);
        if (removed != null) {
            removed.clear();
            log.debug("Purged replay buffer for session {}", sessionId);
        }
    }

    /**
     * Clears all buffers across all sessions.
     */
    public void clearAll() {
        sessionBuffers.clear();
    }
}

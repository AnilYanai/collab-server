package com.anilyanai.engine.session;

import java.util.Collections;
import java.util.List;

/**
 * =========================================================================================
 * 🛡️ StatePreserver (Polymorphic Zero-Condition State Recorder)
 * =========================================================================================
 *
 * <p>Handles replay history preservation for collaborative sessions.</p>
 *
 * <h3>Design & Performance Characteristics:</h3>
 * <ul>
 *   <li><b>Zero Condition Checks in Hot Path:</b> Uses polymorphism / empty method overriding
 *       rather than branching conditions ({@code if (isPreserved)}) on every message.</li>
 *   <li><b>Helper Factory Method:</b> {@link #create(boolean)} returns standard buffering logic
 *       when state is preserved, or a cached singleton empty implementation when state is not preserved.</li>
 *   <li><b>Zero-Allocation Ephemeral Sessions:</b> Ephemeral rooms reuse a static singleton
 *       {@link #NO_OP} instance without allocating unused ring buffer arrays or objects.</li>
 *   <li><b>JIT Inlining:</b> For ephemeral rooms (not preserved), the JVM JIT compiler can inline
 *       the empty {@link #save(byte[])} method into zero machine instructions.</li>
 *   <li><b>Defensive Integrity:</b> Clones raw packet byte arrays on save to prevent external buffer
 *       mutations from corrupting historical session replay.</li>
 * </ul>
 *
 * @author anilyanai
 */
public class StatePreserver {

    /**
     * Cached stateless singleton instance for ephemeral sessions (Zero Allocations).
     */
    public static final StatePreserver NO_OP = new NoOpStatePreserver();

    private final ReplayRingBuffer ringBuffer;

    public StatePreserver() {
        this(ReplayRingBuffer.DEFAULT_CAPACITY);
    }

    public StatePreserver(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive: " + capacity);
        }
        this.ringBuffer = new ReplayRingBuffer(capacity);
    }

    /**
     * Internal constructor allowing subclasses (such as {@link NoOpStatePreserver})
     * to avoid allocating an unused {@link ReplayRingBuffer}.
     */
    protected StatePreserver(ReplayRingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    /**
     * Helper factory method using default capacity:
     * <ul>
     *   <li>If state is preserved: returns an active {@link StatePreserver} instance.</li>
     *   <li>If state is NOT preserved: returns the cached {@link #NO_OP} singleton instance,
     *       ensuring zero allocations and eliminating branching checks in the hot fan-out loop.</li>
     * </ul>
     *
     * @param preserveState true if room history should be saved; false for ephemeral sessions
     * @return active or empty StatePreserver instance
     */
    public static StatePreserver create(boolean preserveState) {
        return create(preserveState, ReplayRingBuffer.DEFAULT_CAPACITY);
    }

    /**
     * Helper factory method with customizable ring buffer capacity:
     *
     * @param preserveState true if room history should be saved; false for ephemeral sessions
     * @param capacity maximum number of frames to preserve in replay history
     * @return active StatePreserver with specified capacity, or cached {@link #NO_OP} instance
     */
    public static StatePreserver create(boolean preserveState, int capacity) {
        if (preserveState) {
            return new StatePreserver(capacity);
        }
        return NO_OP;
    }

    /**
     * Returns the cached singleton empty/no-op preserver.
     */
    public static StatePreserver empty() {
        return NO_OP;
    }

    /**
     * Saves raw packet frame into the replay buffer.
     * Performs a defensive clone to protect preserved history against external mutation.
     */
    public void save(byte[] rawBytes) {
        if (rawBytes != null && ringBuffer != null) {
            ringBuffer.append(rawBytes.clone());
        }
    }

    /**
     * Retrieves unmodifiable snapshot of preserved session history.
     */
    public List<byte[]> getHistory() {
        return ringBuffer != null ? ringBuffer.snapshot() : Collections.emptyList();
    }

    /**
     * Purges preserved session state.
     */
    public void clear() {
        if (ringBuffer != null) {
            ringBuffer.clear();
        }
    }

    /**
     * Dedicated stateless No-Op Preserver implementation.
     * Avoids allocating an internal {@link ReplayRingBuffer} entirely.
     */
    private static final class NoOpStatePreserver extends StatePreserver {

        private NoOpStatePreserver() {
            super(null);
        }

        @Override
        public void save(byte[] rawBytes) {
            // Empty overridden method: zero condition checks, zero allocations, no-op
        }

        @Override
        public List<byte[]> getHistory() {
            return Collections.emptyList();
        }

        @Override
        public void clear() {
            // No-op
        }
    }
}

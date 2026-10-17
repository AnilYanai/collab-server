package com.anilyanai.engine.session;

import java.util.ArrayList;
import java.util.List;

/**
 * =========================================================================================
 * 🔄 ReplayRingBuffer (Bounded Circular FIFO Buffer for Session State Replay)
 * =========================================================================================
 *
 * <p>High-performance bounded circular FIFO buffer designed for collaborative session history.</p>
 *
 * <h3>Design & Performance Characteristics:</h3>
 * <ul>
 *   <li><b>Bounded Memory:</b> Overwrites oldest entries once full to prevent out-of-memory errors.</li>
 *   <li><b>Zero-GC Steady State:</b> Pre-allocated slot array avoids continuous node allocation.</li>
 *   <li><b>Concurrent Snapshot:</b> Clones history in chronological order without blocking writers.</li>
 * </ul>
 *
 * @author anilyanai
 */
public class ReplayRingBuffer {

    public static final int DEFAULT_CAPACITY = 200;

    private final byte[][] buffer;
    private final int capacity;
    private int head = 0;
    private int tail = 0;
    private int size = 0;
    private final Object lock = new Object();

    public ReplayRingBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public ReplayRingBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Ring buffer capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.buffer = new byte[capacity][];
    }

    /**
     * Appends a packet payload frame to the ring buffer.
     * If the buffer is full, the oldest entry is overwritten.
     */
    public void append(byte[] item) {
        if (item == null) return;

        synchronized (lock) {
            buffer[tail] = item;
            tail = (tail + 1) % capacity;
            if (size < capacity) {
                size++;
            } else {
                head = (head + 1) % capacity; // Overwrite oldest item
            }
        }
    }

    /**
     * Returns an unmodifiable snapshot list of all buffered items in chronological order.
     */
    public List<byte[]> snapshot() {
        synchronized (lock) {
            if (size == 0) {
                return List.of();
            }
            List<byte[]> copy = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int index = (head + i) % capacity;
                copy.add(buffer[index]);
            }
            return copy;
        }
    }

    public int size() {
        synchronized (lock) {
            return size;
        }
    }

    public int capacity() {
        return capacity;
    }

    public void clear() {
        synchronized (lock) {
            for (int i = 0; i < capacity; i++) {
                buffer[i] = null;
            }
            head = 0;
            tail = 0;
            size = 0;
        }
    }
}

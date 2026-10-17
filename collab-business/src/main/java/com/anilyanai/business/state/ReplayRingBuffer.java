package com.anilyanai.business.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

/**
 * =========================================================================================
 * 🔄 ReplayRingBuffer (Bounded Circular Buffer for Stateful Rooms)
 * =========================================================================================
 *
 * <p>Preserves the most recent broadcast frames for late joiners in stateful collaboration
 * sessions. Uses an off-heap/direct array or pre-allocated array of raw bytes.</p>
 *
 * @author anilyanai
 */
public final class ReplayRingBuffer {

    private final byte[][] buffer;
    private final int capacity;
    private int writeIndex = 0;
    private int size = 0;
    private final StampedLock lock = new StampedLock();

    public ReplayRingBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.capacity = capacity;
        this.buffer = new byte[capacity][];
    }

    public void append(byte[] frame) {
        if (frame == null || frame.length == 0) return;

        long stamp = lock.writeLock();
        try {
            buffer[writeIndex] = frame;
            writeIndex = (writeIndex + 1) % capacity;
            if (size < capacity) {
                size++;
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public List<byte[]> snapshot() {
        long stamp = lock.readLock();
        try {
            if (size == 0) {
                return Collections.emptyList();
            }

            List<byte[]> list = new ArrayList<>(size);
            int start = (size < capacity) ? 0 : writeIndex;

            for (int i = 0; i < size; i++) {
                int index = (start + i) % capacity;
                byte[] frame = buffer[index];
                if (frame != null) {
                    list.add(frame);
                }
            }
            return Collections.unmodifiableList(list);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public int size() {
        long stamp = lock.readLock();
        try {
            return size;
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public void clear() {
        long stamp = lock.writeLock();
        try {
            for (int i = 0; i < capacity; i++) {
                buffer[i] = null;
            }
            writeIndex = 0;
            size = 0;
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}

package com.anilyanai.connector.connection;

import com.anilyanai.connector.connection.model.ReadResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Connection {

    private static final AtomicInteger GLOBAL_INDEX_COUNTER = new AtomicInteger(0);

    private final String connectionId;
    private final int connectionIndex;

    /**
     * Passive liveness tracker: Timestamp of the most recent network activity (read or write).
     * Updated at zero-allocation cost, avoiding application-level heartbeat polling overhead.
     */
    private volatile long lastActiveTime = System.currentTimeMillis();

    private final Queue<ByteBuffer> outboundQueue =
            new ConcurrentLinkedQueue<>();

    protected Connection(String connectionId) {
        this.connectionId = connectionId;
        this.connectionIndex = GLOBAL_INDEX_COUNTER.getAndIncrement();
    }

    public String getConnectionId() {
        return connectionId;
    }

    /**
     * Monotonically incremented unique connection index.
     * Enables 100% mathematically fair, collision-free round-robin distribution across
     * outbound selectors and UDP egress channel pools, eliminating random hash clustering.
     */
    public int getConnectionIndex() {
        return connectionIndex;
    }

    public long getLastActiveTime() {
        return lastActiveTime;
    }

    public void touchActive() {
        this.lastActiveTime = System.currentTimeMillis();
    }

    public void safeWrite() throws IOException {
        touchActive();
        ByteBuffer buffer;

        while ((buffer = outboundQueue.peek()) != null) {
            write(buffer);

            // If socket buffer is full and bytes remain, leave in queue for next OP_WRITE
            if (buffer.hasRemaining()) {
                break;
            }

            // Entire buffer written successfully, remove from queue
            outboundQueue.poll();
        }
    }

    public ReadResult safeRead() throws IOException {
        touchActive();
        return read();
    }

    public void queueOutboundMessage(ByteBuffer data) {
        outboundQueue.add(data);
    }

    public Queue<ByteBuffer> getOutboundQueue() {
        return outboundQueue;
    }

    public boolean hasPendingData() {
        return !outboundQueue.isEmpty();
    }

    public boolean isWriteOver() {
        return !hasPendingData();
    }

    /**
     * Polymorphic, branchless outbound packet transmission.
     * Eliminates runtime {@code instanceof} checks, type-casting, and repeated hashing
     * on every packet. Each subclass directly transmits via its pre-cached channel or reactor.
     *
     * @param payload raw byte payload to send
     * @throws IOException if network transmission fails
     */
    public abstract void sendOutboundPayload(byte[] payload) throws IOException;

    public abstract void write(ByteBuffer data) throws IOException;

    public abstract ReadResult read() throws IOException;

    public abstract void close() throws IOException;

    public abstract SelectableChannel getChannel();

    /**
     * Polymorphic session membership registration.
     * Eliminates runtime {@code instanceof} checks when joining or leaving sessions.
     */
    public abstract void registerInto(SessionRegistrationTarget target);

    public abstract void unregisterFrom(SessionRegistrationTarget target);

    @Override
    public String toString() {
        return connectionId;
    }
}

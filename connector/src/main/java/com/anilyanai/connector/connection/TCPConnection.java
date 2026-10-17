package com.anilyanai.connector.connection;

import com.anilyanai.connector.connection.model.ReadResult;
import com.anilyanai.connector.selector.OutboundTrafficSelector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class TCPConnection extends Connection {
    private final SocketChannel channel;

    // Buffer to hold incomplete frame fragments across read events
    private ByteBuffer accumulationBuffer;

    // Pre-allocated array for gathering batch writes to avoid array allocations during loop
    private static final int MAX_BATCH_SIZE = 128;
    private final ByteBuffer[] writeBatchArray = new ByteBuffer[MAX_BATCH_SIZE];

    /**
     * Pre-assigned outbound reactor cached ONCE on connection accept.
     * Eliminates repeated hashing, modulo math, and array lookups from the per-packet write path.
     */
    private OutboundTrafficSelector.OutboundReactor assignedReactor;

    /**
     * Pre-registered SelectionKey cached ONCE on connection accept.
     * Eliminates blocking channel.keyFor() lookups and synchronized lock contention on the hot path.
     */
    private SelectionKey outboundKey;

    public TCPConnection(
            String connectionId,
            SocketChannel channel
    ) {
        super(connectionId);
        this.channel = channel;
    }

    public void setAssignedReactor(OutboundTrafficSelector.OutboundReactor assignedReactor) {
        this.assignedReactor = assignedReactor;
    }

    public OutboundTrafficSelector.OutboundReactor getAssignedReactor() {
        return assignedReactor;
    }

    public void setOutboundKey(SelectionKey outboundKey) {
        this.outboundKey = outboundKey;
    }

    public SelectionKey getOutboundKey() {
        return outboundKey;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    /**
     * Enqueues outbound TCP frame to the participant's memory queue and activates write interest
     * on the assigned outbound selector. No direct socket writes are performed on worker threads,
     * ensuring worker threads remain 100% focused on inbound message ingestion.
     */
    @Override
    public void sendOutboundPayload(byte[] payload) throws IOException {
        touchActive();
        ByteBuffer messageBuffer = ByteBuffer.allocate(4 + payload.length);
        messageBuffer.putInt(payload.length);
        messageBuffer.put(payload);
        messageBuffer.flip();

        queueOutboundMessage(messageBuffer);

        if (assignedReactor != null) {
            try {
                assignedReactor.enableWrite(this);
            } catch (ClosedChannelException ignored) {
            }
        }
    }

    /**
     * Zero-copy broadcast queueing:
     * Wraps a pre-framed payload (allocated once per broadcast) into a lightweight buffer wrapper
     * without duplicating the underlying byte array across all recipients.
     */
    public void queueFramedPayload(byte[] framedPayload) {
        touchActive();
        queueOutboundMessage(ByteBuffer.wrap(framedPayload));
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException {
        channel.write(buffer);
    }

    /**
     * High-speed fair gathering batch write:
     * Gathers up to 128 queued message buffers and sends them to the OS kernel in ONE single syscall.
     * Flushes one batch per connection per selector cycle and yields immediately to give equal
     * CPU and network bandwidth to all other connections, preventing socket starvation.
     */
    @Override
    public void safeWrite() throws IOException {
        Queue<ByteBuffer> queue = getOutboundQueue();
        if (queue.isEmpty()) {
            return;
        }

        int count = 0;
        for (ByteBuffer buffer : queue) {
            writeBatchArray[count++] = buffer;
            if (count == MAX_BATCH_SIZE) {
                break;
            }
        }

        if (count == 0) {
            return;
        }

        if (count == 1) {
            ByteBuffer single = writeBatchArray[0];
            int written = channel.write(single);
            if (written > 0 && !single.hasRemaining()) {
                queue.poll();
            }
        } else {
            long written = channel.write(writeBatchArray, 0, count);
            if (written > 0) {
                for (int i = 0; i < count; i++) {
                    ByteBuffer buf = writeBatchArray[i];
                    if (!buf.hasRemaining()) {
                        queue.poll();
                    } else {
                        break; // Partial buffer remaining, keep in queue for next turn
                    }
                }
            }
        }
    }

    @Override
    public ReadResult read() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        int bytesRead = channel.read(buffer);

        if (bytesRead == -1) {
            close();
            return null;
        }

        buffer.flip();

        return new ReadResult(
                buffer,
                null
        );
    }

    /**
     * Extracts all complete length-prefixed frames from incoming raw bytes.
     * Protocol frame format: [4-byte integer length N][N bytes payload]
     * Handles coalesced packets, partial headers, and partial payloads across reads.
     */
    public List<byte[]> extractFrames(ByteBuffer readBuffer) {
        List<byte[]> frames = new ArrayList<>();

        ByteBuffer source;
        if (accumulationBuffer != null && accumulationBuffer.position() > 0) {
            accumulationBuffer.flip();
            ByteBuffer merged = ByteBuffer.allocate(accumulationBuffer.remaining() + readBuffer.remaining());
            merged.put(accumulationBuffer);
            merged.put(readBuffer);
            merged.flip();
            source = merged;
            accumulationBuffer = null;
        } else {
            source = readBuffer;
        }

        while (source.remaining() >= 4) {
            source.mark();
            int frameLength = source.getInt();

            if (frameLength < 0 || frameLength > 16 * 1024 * 1024) {
                throw new IllegalStateException("Invalid TCP frame length: " + frameLength);
            }

            if (source.remaining() >= frameLength) {
                byte[] frame = new byte[frameLength];
                source.get(frame);
                frames.add(frame);
            } else {
                source.reset();
                break;
            }
        }

        if (source.hasRemaining()) {
            accumulationBuffer = ByteBuffer.allocate(Math.max(1024, source.remaining() * 2));
            accumulationBuffer.put(source);
        }

        return frames;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public void registerInto(SessionRegistrationTarget target) {
        if (target != null) {
            target.registerTCP(this);
        }
    }

    @Override
    public void unregisterFrom(SessionRegistrationTarget target) {
        if (target != null) {
            target.unregisterTCP(getConnectionId());
        }
    }
}

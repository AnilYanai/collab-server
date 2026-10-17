package com.anilyanai.connector.transport;

import com.anilyanai.connector.connection.TCPConnection;
import com.anilyanai.connector.selector.OutboundTrafficSelector;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.util.Collection;

/**
 * =========================================================================================
 * 🚀 TCPBroadcastHelper (Dedicated High-Performance TCP Broadcast Dispatcher)
 * =========================================================================================
 *
 * <p>Handles reliable TCP stream fan-out. Streams 4-byte length-prefixed frames to all
 * active TCP participants with zero instanceof checks and zero type-casting.</p>
 */
@Slf4j
public class TCPBroadcastHelper {

    private static final int MAX_REACTORS = 64;

    /**
     * Dedicated high-speed broadcast path for reliable TCP streams.
     * Frames payload ONCE for all room recipients, enqueues to participant queues,
     * caches the touched selectors, and triggers exactly ONE wakeup call per selector
     * at the end of the broadcast.
     *
     * @param payload raw byte payload to broadcast
     * @param targets collection of recipient TCP connections
     * @param senderConnectionId sender ID to exclude (or null to broadcast to all)
     */
    public static void broadcastPayload(byte[] payload, Collection<TCPConnection> targets, String senderConnectionId) {
        if (targets == null || targets.isEmpty() || payload == null || payload.length == 0) {
            return;
        }

        // 1. Frame payload ONCE for all recipients in the room: [4-byte length][payload bytes]
        byte[] framed = new byte[4 + payload.length];
        framed[0] = (byte) ((payload.length >> 24) & 0xFF);
        framed[1] = (byte) ((payload.length >> 16) & 0xFF);
        framed[2] = (byte) ((payload.length >> 8) & 0xFF);
        framed[3] = (byte) (payload.length & 0xFF);
        System.arraycopy(payload, 0, framed, 4, payload.length);

        // Cache touched selectors to issue exactly ONE wakeup call per selector for this broadcast
        OutboundTrafficSelector.OutboundReactor[] touchedReactors = new OutboundTrafficSelector.OutboundReactor[MAX_REACTORS];
        int touchedCount = 0;
        long touchedBitmask = 0L;

        for (TCPConnection target : targets) {
            if (target == null) continue;
            if (senderConnectionId != null && senderConnectionId.equals(target.getConnectionId())) {
                continue; // Skip echo to sender
            }

            target.touchActive();
            target.queueOutboundMessage(ByteBuffer.wrap(framed));

            SelectionKey key = target.getOutboundKey();
            if (key != null && key.isValid()) {
                try {
                    int currentOps = key.interestOps();
                    if ((currentOps & SelectionKey.OP_WRITE) == 0) {
                        key.interestOpsOr(SelectionKey.OP_WRITE);
                    }
                } catch (CancelledKeyException ignored) {
                }
            }

            OutboundTrafficSelector.OutboundReactor reactor = target.getAssignedReactor();
            if (reactor != null) {
                int index = reactor.getIndex();
                long bit = (1L << (index & 63));
                if ((touchedBitmask & bit) == 0L) {
                    touchedBitmask |= bit;
                    if (touchedCount < MAX_REACTORS) {
                        touchedReactors[touchedCount++] = reactor;
                    }
                }
            }
        }

        // 2. Exactly ONE wakeup call per touched selector for this entire broadcast
        for (int i = 0; i < touchedCount; i++) {
            touchedReactors[i].wakeupSelector();
        }
    }
}

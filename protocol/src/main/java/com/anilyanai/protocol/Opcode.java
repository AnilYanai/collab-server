package com.anilyanai.protocol;

import java.util.HashMap;
import java.util.Map;

/**
 * Universal infrastructure-level operations supported by the collaboration server.
 * <p>
 * The server is strictly domain-agnostic and does not inspect application business logic.
 * These opcodes represent low-level session routing, discovery, messaging, and lifecycle actions.
 * Liveness and heartbeat checks are handled at the transport layer (TCP SO_KEEPALIVE) to preserve
 * maximum wire-speed fan-out without polluting the collaboration protocol.
 * </p>
 */
public enum Opcode {

    /**
     * Client requests to join a specific collaboration session/room.
     * Authorized via participant session token.
     */
    JOIN_SESSION((byte) 0x01),

    /**
     * Client requests to leave a collaboration session/room.
     */
    LEAVE_SESSION((byte) 0x02),

    /**
     * Broadcasts the opaque payload to ALL other clients in the session (excluding sender).
     * Used for drawing strokes, game ticks, cursor movement, chat, CRDT edits, etc.
     */
    BROADCAST((byte) 0x03),

    /**
     * Broadcasts the opaque payload to ALL clients in the session INCLUDING the sender.
     * Useful for global state sync or sequenced shared-log events.
     */
    BROADCAST_ALL((byte) 0x04),

    /**
     * Targeted point-to-point message directed to a specific client inside the session.
     * Useful for private messaging, WebRTC P2P/SFU signaling, or individual state sync.
     */
    DIRECT((byte) 0x05),

    /**
     * Server acknowledgement of a successful operation.
     */
    ACK((byte) 0x06),

    /**
     * Server error response (e.g. invalid session, malformed packet, unauthorized).
     */
    ERROR((byte) 0x07),

    /**
     * Administrative request to close/terminate an entire session for all participants.
     * Requires high-privilege Master Admin token. Server notifies all members before tearing down.
     */
    CLOSE_SESSION((byte) 0x08),

    /**
     * Client explicitly requests the preserved state / replay history of a session.
     */
    GET_STATE((byte) 0x09);

    private final byte code;

    private static final Map<Byte, Opcode> BY_CODE = new HashMap<>();

    static {
        for (Opcode opcode : values()) {
            BY_CODE.put(opcode.code, opcode);
        }
    }

    Opcode(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    /**
     * Resolves an {@link Opcode} from its single-byte identifier.
     *
     * @param code the byte code representing the opcode
     * @return the matching {@link Opcode}, or {@code null} if unrecognized
     */
    public static Opcode fromCode(byte code) {
        return BY_CODE.get(code);
    }
}

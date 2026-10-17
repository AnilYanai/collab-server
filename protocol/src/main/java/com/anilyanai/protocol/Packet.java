package com.anilyanai.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable representation of a protocol packet.
 * <p>
 * The server is application-agnostic: the {@code payload} is an opaque {@code byte[]}
 * carrying application domain data (CRDT edits, drawing paths, game coordinates, audio/video chunks, or chat).
 * </p>
 *
 * @param version     protocol version (currently 1)
 * @param opcode      action command (JOIN, LEAVE, BROADCAST, DIRECT, CLOSE_SESSION, GET_STATE, etc.)
 * @param flags       bitmask flags (chunking, direct routing, delivery preference)
 * @param messageId   unique identifier for the message / chunk stream
 * @param chunkIndex  0-based index of this chunk (0 if unchunked)
 * @param totalChunks total number of chunks for the message (1 if unchunked)
 * @param sessionId   target room/session identifier (empty string if not applicable)
 * @param targetId    specific client connection ID for point-to-point DIRECT messages, or null
 * @param payload     opaque application payload bytes
 */
public record Packet(
        byte version,
        Opcode opcode,
        byte flags,
        int messageId,
        short chunkIndex,
        short totalChunks,
        String sessionId,
        String targetId,
        byte[] payload
) {
    public static final byte CURRENT_VERSION = 1;
    public static final byte MAGIC_BYTE = (byte) 0xCA;

    public static final byte FLAG_NONE = 0x00;
    public static final byte FLAG_CHUNKED = 0x01;
    public static final byte FLAG_DIRECT = 0x02;

    /**
     * Session State Preservation Flag:
     * When set on a JOIN_SESSION packet by the room creator, instructs
     * the server that this session is stateful and all broadcast messages should be
     * preserved in a circular replay buffer for late comers.
     * If not set (default 0x00), the session is ephemeral (e.g. video call) and keeps 0 state.
     */
    public static final byte FLAG_PRESERVE_STATE = 0x04;

    public Packet {
        Objects.requireNonNull(opcode, "Opcode cannot be null");
        if (sessionId == null) sessionId = "";
        if (payload == null) payload = new byte[0];
    }

    /**
     * Creates a standard session broadcast packet.
     *
     * @param sessionId destination session
     * @param payload   application opaque payload
     * @return Packet configured for session broadcast
     */
    public static Packet ofBroadcast(String sessionId, byte[] payload) {
        return new Packet(
                CURRENT_VERSION,
                Opcode.BROADCAST,
                FLAG_NONE,
                0,
                (short) 0,
                (short) 1,
                sessionId,
                null,
                payload
        );
    }

    /**
     * Creates a broadcast packet that also loops back to the sender.
     *
     * @param sessionId destination session
     * @param payload   application opaque payload
     * @return Packet configured for global broadcast
     */
    public static Packet ofBroadcastAll(String sessionId, byte[] payload) {
        return new Packet(
                CURRENT_VERSION,
                Opcode.BROADCAST_ALL,
                FLAG_NONE,
                0,
                (short) 0,
                (short) 1,
                sessionId,
                null,
                payload
        );
    }

    /**
     * Creates a direct targeted packet to a specific member of the session.
     *
     * @param sessionId destination session
     * @param targetId  specific client connection ID
     * @param payload   application opaque payload
     * @return Packet configured for direct delivery
     */
    public static Packet ofDirect(String sessionId, String targetId, byte[] payload) {
        return new Packet(
                CURRENT_VERSION,
                Opcode.DIRECT,
                FLAG_DIRECT,
                0,
                (short) 0,
                (short) 1,
                sessionId,
                targetId,
                payload
        );
    }

    /**
     * Creates a session join request packet (ephemeral by default).
     *
     * @param sessionId session to join
     * @return Packet configured for joining
     */
    public static Packet ofJoin(String sessionId) {
        return ofJoin(sessionId, false);
    }

    /**
     * Creates a session join request packet with explicit state preservation configuration.
     *
     * @param sessionId     session to join
     * @param preserveState true if this session should preserve data for late comers; false for ephemeral
     * @return Packet configured for joining
     */
    public static Packet ofJoin(String sessionId, boolean preserveState) {
        byte flags = preserveState ? FLAG_PRESERVE_STATE : FLAG_NONE;
        return new Packet(
                CURRENT_VERSION,
                Opcode.JOIN_SESSION,
                flags,
                0,
                (short) 0,
                (short) 1,
                sessionId,
                null,
                new byte[0]
        );
    }

    /**
     * Creates a session leave request packet.
     *
     * @param sessionId session to leave
     * @return Packet configured for leaving
     */
    public static Packet ofLeave(String sessionId) {
        return new Packet(
                CURRENT_VERSION,
                Opcode.LEAVE_SESSION,
                FLAG_NONE,
                0,
                (short) 0,
                (short) 1,
                sessionId,
                null,
                new byte[0]
        );
    }

    /**
     * Creates an explicit request to fetch preserved session state/history.
     *
     * @param sessionId session whose state is requested
     * @return Packet configured for GET_STATE
     */
    public static Packet ofGetState(String sessionId) {
        return new Packet(
                CURRENT_VERSION,
                Opcode.GET_STATE,
                FLAG_NONE,
                0,
                (short) 0,
                (short) 1,
                sessionId,
                null,
                new byte[0]
        );
    }

    /**
     * Creates an administrative session close request packet.
     *
     * @param sessionId  session to close
     * @param adminToken cryptographic admin authorization token payload
     * @return Packet configured for closing the session
     */
    public static Packet ofCloseSession(String sessionId, byte[] adminToken) {
        return new Packet(
                CURRENT_VERSION,
                Opcode.CLOSE_SESSION,
                FLAG_NONE,
                0,
                (short) 0,
                (short) 1,
                sessionId,
                null,
                adminToken != null ? adminToken : new byte[0]
        );
    }

    /**
     * Creates a chunked packet for transmitting large payloads (video, canvas snapshot, files) in fragments.
     *
     * @param opcode      action command (e.g. BROADCAST or DIRECT)
     * @param sessionId   destination session
     * @param targetId    target client ID (if direct), or null
     * @param messageId   message identifier shared across all chunks of the same message
     * @param chunkIndex  0-based chunk index
     * @param totalChunks total number of chunks
     * @param chunkData   slice of payload bytes
     * @return Packet configured as a chunk fragment
     */
    public static Packet ofChunk(
            Opcode opcode,
            String sessionId,
            String targetId,
            int messageId,
            short chunkIndex,
            short totalChunks,
            byte[] chunkData
    ) {
        byte flags = FLAG_CHUNKED;
        if (targetId != null && !targetId.isBlank()) {
            flags |= FLAG_DIRECT;
        }

        return new Packet(
                CURRENT_VERSION,
                opcode,
                flags,
                messageId,
                chunkIndex,
                totalChunks,
                sessionId,
                targetId,
                chunkData
        );
    }

    public boolean isChunked() {
        return (flags & FLAG_CHUNKED) != 0;
    }

    public boolean isDirect() {
        return (flags & FLAG_DIRECT) != 0;
    }

    public boolean isStatePreserved() {
        return (flags & FLAG_PRESERVE_STATE) != 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Packet packet)) return false;
        return version == packet.version &&
                flags == packet.flags &&
                messageId == packet.messageId &&
                chunkIndex == packet.chunkIndex &&
                totalChunks == packet.totalChunks &&
                opcode == packet.opcode &&
                Objects.equals(sessionId, packet.sessionId) &&
                Objects.equals(targetId, packet.targetId) &&
                Arrays.equals(payload, packet.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(version, opcode, flags, messageId, chunkIndex, totalChunks, sessionId, targetId);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "Packet{" +
                "version=" + version +
                ", opcode=" + opcode +
                ", flags=" + flags +
                ", messageId=" + messageId +
                ", chunk=" + (chunkIndex + 1) + "/" + totalChunks +
                ", sessionId='" + sessionId + '\'' +
                ", targetId='" + targetId + '\'' +
                ", payloadLength=" + payload.length +
                '}';
    }
}

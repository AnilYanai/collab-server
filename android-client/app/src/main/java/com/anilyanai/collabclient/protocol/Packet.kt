package com.anilyanai.collabclient.protocol

/**
 * Immutable representation of a collaboration protocol packet on Android.
 */
data class Packet(
    val version: Byte = CURRENT_VERSION,
    val opcode: Opcode,
    val flags: Byte = FLAG_NONE,
    val messageId: Int = 0,
    val chunkIndex: Short = 0,
    val totalChunks: Short = 1,
    val sessionId: String = "",
    val targetId: String? = null,
    val payload: ByteArray = ByteArray(0)
) {
    companion object {
        const val CURRENT_VERSION: Byte = 1
        const val MAGIC_BYTE: Byte = 0xCA.toByte()

        const val FLAG_NONE: Byte = 0x00
        const val FLAG_CHUNKED: Byte = 0x01
        const val FLAG_DIRECT: Byte = 0x02
        const val FLAG_PRESERVE_STATE: Byte = 0x04

        fun ofBroadcast(sessionId: String, payload: ByteArray): Packet {
            return Packet(
                version = CURRENT_VERSION,
                opcode = Opcode.BROADCAST,
                flags = FLAG_NONE,
                sessionId = sessionId,
                payload = payload
            )
        }

        fun ofJoin(sessionId: String, token: ByteArray): Packet {
            return Packet(
                version = CURRENT_VERSION,
                opcode = Opcode.JOIN_SESSION,
                flags = FLAG_NONE,
                sessionId = sessionId,
                payload = token
            )
        }

        fun ofLeave(sessionId: String): Packet {
            return Packet(
                version = CURRENT_VERSION,
                opcode = Opcode.LEAVE_SESSION,
                flags = FLAG_NONE,
                sessionId = sessionId
            )
        }
    }

    val isChunked: Boolean get() = (flags.toInt() and FLAG_CHUNKED.toInt()) != 0
    val isDirect: Boolean get() = (flags.toInt() and FLAG_DIRECT.toInt()) != 0

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Packet) return false
        return version == other.version &&
                opcode == other.opcode &&
                flags == other.flags &&
                messageId == other.messageId &&
                chunkIndex == other.chunkIndex &&
                totalChunks == other.totalChunks &&
                sessionId == other.sessionId &&
                targetId == other.targetId &&
                payload.contentEquals(other.payload)
    }

    override fun hashCode(): Int {
        var result = version.toInt()
        result = 31 * result + opcode.hashCode()
        result = 31 * result + flags.toInt()
        result = 31 * result + messageId
        result = 31 * result + chunkIndex.toInt()
        result = 31 * result + totalChunks.toInt()
        result = 31 * result + sessionId.hashCode()
        result = 31 * result + (targetId?.hashCode() ?: 0)
        result = 31 * result + payload.contentHashCode()
        return result
    }
}

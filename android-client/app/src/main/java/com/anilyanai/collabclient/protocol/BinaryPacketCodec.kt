package com.anilyanai.collabclient.protocol

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Binary Packet Codec for Android matching the server wire protocol byte-for-byte.
 */
class BinaryPacketCodec {

    companion object {
        const val MIN_PACKET_HEADER_SIZE = 17 // 1+1+1+1+4+2+2+1+4
        const val MAX_SESSION_ID_LENGTH = 255
        const val MAX_TARGET_ID_LENGTH = 255
    }

    fun encode(packet: Packet): ByteArray {
        val sessionBytes = packet.sessionId.toByteArray(StandardCharsets.UTF_8)
        require(sessionBytes.size <= MAX_SESSION_ID_LENGTH) { "Session ID exceeds 255 bytes" }

        val isDirect = packet.isDirect && !packet.targetId.isNullOrBlank()
        val targetBytes = if (isDirect) packet.targetId!!.toByteArray(StandardCharsets.UTF_8) else ByteArray(0)
        require(targetBytes.size <= MAX_TARGET_ID_LENGTH) { "Target ID exceeds 255 bytes" }

        var flags = packet.flags
        flags = if (isDirect) {
            (flags.toInt() or Packet.FLAG_DIRECT.toInt()).toByte()
        } else {
            (flags.toInt() and Packet.FLAG_DIRECT.toInt().inv()).toByte()
        }

        val payload = packet.payload
        val totalSize = 1 + 1 + 1 + 1 + 4 + 2 + 2 + 1 + sessionBytes.size +
                (if (isDirect) 1 + targetBytes.size else 0) +
                4 + payload.size

        val buffer = ByteBuffer.allocate(totalSize)

        // Header
        buffer.put(Packet.MAGIC_BYTE)
        buffer.put(packet.version)
        buffer.put(packet.opcode.code)
        buffer.put(flags)
        buffer.putInt(packet.messageId)
        buffer.putShort(packet.chunkIndex)
        buffer.putShort(packet.totalChunks)

        // Session ID
        buffer.put(sessionBytes.size.toByte())
        if (sessionBytes.isNotEmpty()) {
            buffer.put(sessionBytes)
        }

        // Target ID
        if (isDirect) {
            buffer.put(targetBytes.size.toByte())
            if (targetBytes.isNotEmpty()) {
                buffer.put(targetBytes)
            }
        }

        // Payload
        buffer.putInt(payload.size)
        if (payload.isNotEmpty()) {
            buffer.put(payload)
        }

        return buffer.array()
    }

    fun decode(rawData: ByteArray): Packet {
        require(rawData.size >= MIN_PACKET_HEADER_SIZE) { "Packet too small" }
        val buffer = ByteBuffer.wrap(rawData)

        val magic = buffer.get()
        require(magic == Packet.MAGIC_BYTE) { "Invalid magic byte: $magic" }

        val version = buffer.get()
        require(version == Packet.CURRENT_VERSION) { "Unsupported version: $version" }

        val opcodeByte = buffer.get()
        val opcode = Opcode.fromCode(opcodeByte) ?: throw IllegalArgumentException("Unknown opcode: $opcodeByte")

        val flags = buffer.get()
        val messageId = buffer.int
        val chunkIndex = buffer.short
        var totalChunks = buffer.short
        if (totalChunks <= 0) totalChunks = 1

        val sessionLen = buffer.get().toInt() and 0xFF
        require(buffer.remaining() >= sessionLen) { "Truncated session ID" }
        val sessionId = if (sessionLen > 0) {
            val bytes = ByteArray(sessionLen)
            buffer.get(bytes)
            String(bytes, StandardCharsets.UTF_8)
        } else ""

        val isDirect = (flags.toInt() and Packet.FLAG_DIRECT.toInt()) != 0
        var targetId: String? = null
        if (isDirect) {
            val targetLen = buffer.get().toInt() and 0xFF
            require(buffer.remaining() >= targetLen) { "Truncated target ID" }
            if (targetLen > 0) {
                val bytes = ByteArray(targetLen)
                buffer.get(bytes)
                targetId = String(bytes, StandardCharsets.UTF_8)
            }
        }

        val payloadLen = buffer.int
        require(payloadLen >= 0) { "Negative payload length" }
        require(buffer.remaining() >= payloadLen) { "Truncated payload" }

        val payload = if (payloadLen > 0) {
            val bytes = ByteArray(payloadLen)
            buffer.get(bytes)
            bytes
        } else ByteArray(0)

        return Packet(
            version = version,
            opcode = opcode,
            flags = flags,
            messageId = messageId,
            chunkIndex = chunkIndex,
            totalChunks = totalChunks,
            sessionId = sessionId,
            targetId = targetId,
            payload = payload
        )
    }
}

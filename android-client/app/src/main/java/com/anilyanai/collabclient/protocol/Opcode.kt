package com.anilyanai.collabclient.protocol

enum class Opcode(val code: Byte) {
    JOIN_SESSION(0x01),
    LEAVE_SESSION(0x02),
    BROADCAST(0x03),
    BROADCAST_ALL(0x04),
    DIRECT(0x05),
    ACK(0x06),
    ERROR(0x07),
    CLOSE_SESSION(0x08),
    GET_STATE(0x09);

    companion object {
        private val BY_CODE = entries.associateBy { it.code }
        fun fromCode(code: Byte): Opcode? = BY_CODE[code]
    }
}

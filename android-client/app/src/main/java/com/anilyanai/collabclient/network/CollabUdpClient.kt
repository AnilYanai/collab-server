package com.anilyanai.collabclient.network

import com.anilyanai.collabclient.protocol.BinaryPacketCodec
import com.anilyanai.collabclient.protocol.Opcode
import com.anilyanai.collabclient.protocol.Packet
import com.anilyanai.collabclient.security.HmacTokenAuthorizer
import kotlinx.coroutines.*
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * High-performance UDP Client managing datagram transmission for both Chat and Real-Time Video.
 */
class CollabUdpClient(
    private val onJoined: (sessionId: String) -> Unit,
    private val onMessageReceived: (senderId: String, text: String, latencyMs: Long) -> Unit,
    private val onVideoFrameReceived: (senderId: String, jpegBytes: ByteArray, latencyMs: Long) -> Unit,
    private val onError: (message: String) -> Unit,
    private val onStatusChanged: (status: String) -> Unit
) {
    companion object {
        const val PAYLOAD_TAG_CHAT: Byte = 0x01
        const val PAYLOAD_TAG_VIDEO: Byte = 0x02
    }

    private val codec = BinaryPacketCodec()
    private var socket: DatagramSocket? = null
    private var serverAddress: InetSocketAddress? = null
    private var currentSessionId: String = ""
    private var currentUserId: String = ""
    private var isJoined = AtomicBoolean(false)
    private var running = AtomicBoolean(false)
    private val frameSequence = AtomicInteger(0)

    private val clientScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    fun connectAndJoin(
        host: String,
        port: Int,
        sessionId: String,
        userId: String,
        secretKey: String
    ) {
        disconnect()

        clientScope.launch {
            try {
                onStatusChanged("Connecting to $host:$port via UDP...")
                currentSessionId = sessionId
                currentUserId = userId
                serverAddress = InetSocketAddress(host, port)

                socket = DatagramSocket().apply {
                    receiveBufferSize = 1024 * 1024 // 1 MB buffer for video streaming
                    sendBufferSize = 1024 * 1024
                }
                running.set(true)

                // Start inbound listener coroutine
                launch { readLoop() }

                // Generate signed HMAC token
                val token = HmacTokenAuthorizer.generateToken(
                    sessionId = sessionId,
                    userId = userId,
                    durationSeconds = 3600,
                    secretKey = secretKey
                )

                val tokenBytes = token.toByteArray(StandardCharsets.UTF_8)
                val joinPacket = Packet.ofJoin(sessionId, tokenBytes)
                sendPacket(joinPacket)

                onStatusChanged("Join request sent. Awaiting ACK...")

            } catch (e: Exception) {
                onError("Connection error: ${e.message}")
                onStatusChanged("Disconnected")
            }
        }
    }

    fun sendBroadcastMessage(text: String) {
        if (!isJoined.get() || socket == null) {
            onError("Not connected to a session")
            return
        }

        clientScope.launch {
            try {
                val now = System.currentTimeMillis()
                val chatPayload = "[CHAT]|$currentUserId|$currentSessionId|1|$now|$text"
                val packet = Packet.ofBroadcast(
                    currentSessionId,
                    chatPayload.toByteArray(StandardCharsets.UTF_8)
                )
                sendPacket(packet)
            } catch (e: Exception) {
                onError("Failed to send message: ${e.message}")
            }
        }
    }

    /**
     * Transmits a live compressed video frame to all participants in the session.
     */
    fun sendVideoFrame(jpegBytes: ByteArray) {
        if (!isJoined.get() || socket == null || !running.get()) return

        try {
            val now = System.currentTimeMillis()
            val seq = frameSequence.incrementAndGet()

            // Wire format: [TAG (1B)][NOW (8B)][SEQ (4B)][JPEG DATA (N bytes)]
            val buffer = ByteBuffer.allocate(1 + 8 + 4 + jpegBytes.size)
            buffer.put(PAYLOAD_TAG_VIDEO)
            buffer.putLong(now)
            buffer.putInt(seq)
            buffer.put(jpegBytes)

            val packet = Packet.ofBroadcast(currentSessionId, buffer.array())
            sendPacket(packet)
        } catch (_: Exception) {
            // Drop frame on transient network backpressure
        }
    }

    private fun sendPacket(packet: Packet) {
        val bytes = codec.encode(packet)
        val datagram = DatagramPacket(bytes, bytes.size, serverAddress)
        socket?.send(datagram)
    }

    private fun readLoop() {
        val buffer = ByteArray(65535) // Accept maximum UDP frame size
        val datagram = DatagramPacket(buffer, buffer.size)

        while (running.get()) {
            try {
                socket?.receive(datagram)
                val recvTime = System.currentTimeMillis()

                val rawData = datagram.data.copyOf(datagram.length)
                val packet = codec.decode(rawData)

                when (packet.opcode) {
                    Opcode.ACK -> {
                        if (isJoined.compareAndSet(false, true)) {
                            onStatusChanged("Joined: $currentSessionId")
                            onJoined(currentSessionId)
                        }
                    }
                    Opcode.ERROR -> {
                        val errorText = String(packet.payload, StandardCharsets.UTF_8)
                        onError("Server Error: $errorText")
                    }
                    Opcode.BROADCAST, Opcode.BROADCAST_ALL -> {
                        handleInboundPayload(packet.payload, recvTime)
                    }
                    else -> {}
                }
            } catch (e: Exception) {
                if (running.get()) {
                    onError("Read error: ${e.message}")
                }
                break
            }
        }
    }

    private fun handleInboundPayload(payload: ByteArray, recvTime: Long) {
        if (payload.isEmpty()) return

        // 1. Check for binary video frame tag (0x02)
        if (payload[0] == PAYLOAD_TAG_VIDEO && payload.size > 13) {
            val buf = ByteBuffer.wrap(payload, 1, payload.size - 1)
            val sentTime = buf.long
            val seq = buf.int
            val latency = (recvTime - sentTime).coerceAtLeast(0)

            val jpegBytes = ByteArray(payload.size - 13)
            buf.get(jpegBytes)

            onVideoFrameReceived("Peer", jpegBytes, latency)
            return
        }

        // 2. Chat or text broadcast
        val rawText = String(payload, StandardCharsets.UTF_8)
        val parts = rawText.split("|", limit = 6)

        if (parts.size >= 6 && parts[0] == "[CHAT]") {
            val sender = parts[1]
            val timestamp = parts[4].toLongOrNull() ?: recvTime
            val messageContent = parts[5]
            val latency = (recvTime - timestamp).coerceAtLeast(0)

            onMessageReceived(sender, messageContent, latency)
        } else {
            onMessageReceived("Peer", rawText, 0)
        }
    }

    fun disconnect() {
        running.set(false)
        isJoined.set(false)
        try {
            if (currentSessionId.isNotEmpty()) {
                val leavePacket = Packet.ofLeave(currentSessionId)
                sendPacket(leavePacket)
            }
            socket?.close()
        } catch (_: Exception) {
        } finally {
            socket = null
            onStatusChanged("Disconnected")
        }
    }
}

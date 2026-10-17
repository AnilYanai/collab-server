package com.anilyanai.videosimulation;

import com.anilyanai.business.security.HmacTokenAuthorizer;
import com.anilyanai.protocol.Opcode;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.BinaryPacketCodec;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * =========================================================================================
 * 🔥 4-CLIENT GROUP VIDEO MEETING BENCHMARK: 27 SESSIONS x 4 CLIENTS (108 TOTAL CLIENTS)
 * =========================================================================================
 *
 * <p>High-performance simulation of 27 isolated 4-participant group video rooms:
 * <ul>
 *   <li><b>27 Independent Group Sessions:</b> Exactly 4 participants per session (27 rooms x 4 = 108 participants).</li>
 *   <li><b>1. Preserved Session (TCP):</b> {@code [room-id]-preserved-chat} for reliable chat history.</li>
 *   <li><b>2. Unpreserved Session (UDP):</b> {@code [room-id]-unpreserved-media} for Audio (0x01) + Video (0x02) multiplexing.</li>
 *   <li><b>1-to-3 Fan-Out:</b> Each participant's stream is broadcast to the other 3 peers (324 concurrent fan-out streams).</li>
 * </ul>
 * </p>
 */
public class FourClientVideoMeetingSimulationBenchmark {

    // Network & Room Base Config
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 8080;
    private static final String BASE_ROOM_ID = "group4-room-" + (System.currentTimeMillis() % 1_000_000);
    private static final String HMAC_SECRET = "collab-benchmark-secret-key-256bit-ok";

    // 4-Client Group Topology: 27 sessions x 4 clients = 108 participants
    public static final int NUM_SESSIONS = 27;
    public static final int CLIENTS_PER_SESSION = 4; // Exactly 4 clients per group call
    public static final int TOTAL_CLIENTS = NUM_SESSIONS * CLIENTS_PER_SESSION;
    public static final int ACTIVE_MEDIA_PUBLISHERS = TOTAL_CLIENTS; // ALL 108 participants stream A/V
    public static final int SIMULATION_DURATION_SEC = 10; // Run for 10 seconds

    // Payload Stream Tags
    private static final byte TAG_AUDIO = 0x01; // Unpreserved Opus Audio (20ms frames)
    private static final byte TAG_VIDEO = 0x02; // Unpreserved 1080p Video (30 FPS)

    // Media Parameters
    private static final int AUDIO_PACKET_SIZE = 160; // 160-byte Opus audio frame (20ms = 50 pkts/sec)
    private static final int TARGET_FPS = 30; // 30 FPS video
    private static final long VIDEO_FRAME_DURATION_NANOS = 1_000_000_000L / TARGET_FPS; // ~33.3 ms
    private static final int VIDEO_CHUNK_SIZE = 1200; // MTU-safe 1,200 bytes per UDP datagram
    private static final int VIDEO_CHUNKS_PER_FRAME = 4; // 4 * 1,200 = 4.8 KB per frame (~1.15 Mbps)

    // Codec & State
    private static final BinaryPacketCodec CODEC = new BinaryPacketCodec();
    private static final AtomicBoolean running = new AtomicBoolean(true);

    // Global Metrics
    private static final AtomicLong videoChunksSent = new AtomicLong(0);
    private static final AtomicLong videoChunksReceived = new AtomicLong(0);
    private static final AtomicLong audioPktsSent = new AtomicLong(0);
    private static final AtomicLong audioPktsReceived = new AtomicLong(0);
    private static final AtomicLong mediaBytesReceived = new AtomicLong(0);
    private static final AtomicLong chatSent = new AtomicLong(0);
    private static final AtomicLong chatReceived = new AtomicLong(0);
    private static final ConcurrentLinkedQueue<Long> videoLatencySamples = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<Long> audioLatencySamples = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws Exception {
        System.out.println("=========================================================================================");
        System.out.println("🔥 4-CLIENT GROUP DUAL-SESSION VIDEO BENCHMARK (" + NUM_SESSIONS + " ROOMS x 4 CLIENTS = " + TOTAL_CLIENTS + " TOTAL PARTICIPANTS)");
        System.out.println("=========================================================================================");
        System.out.println("Target Server            : " + HOST + ":" + PORT + " (Dual Stack: UDP Media + TCP Chat)");
        System.out.println("Total Rooms              : " + NUM_SESSIONS + " group rooms");
        System.out.println("Participants per Room    : " + CLIENTS_PER_SESSION + " clients (4-Way Mesh Call)");
        System.out.println("Total Active Clients     : " + TOTAL_CLIENTS);
        System.out.println("Architecture Separation  : 2 SESSIONS PER ROOM");
        System.out.println("  • Session 1 [PRESERVED]  : '[room-id]-preserved-chat' (TCP stream for reliable chat)");
        System.out.println("  • Session 2 [UNPRESERVED]: '[room-id]-unpreserved-media' (UDP multiplexed Audio + Video)");
        System.out.println("Audio Stream (UDP)       : Opus Voice ~64 Kbps (Tag: 0x01, 50 packets/sec per user)");
        System.out.println("Video Stream (UDP)       : 1080p Full HD @ 30 FPS ~1.15 Mbps (Tag: 0x02, 120 packets/sec per user)");
        System.out.println("Chat Stream (TCP)        : 4 chats/sec across all rooms (preserved reliable stream)");
        System.out.println("Concurrent Video Streams : " + (NUM_SESSIONS * CLIENTS_PER_SESSION * (CLIENTS_PER_SESSION - 1)) + " fan-out video streams (" + NUM_SESSIONS + " rooms x 4x3 mesh)");
        System.out.printf("Inbound Video Rate       : %,d packets/sec (%,.2f Mbps into server)%n",
                TOTAL_CLIENTS * TARGET_FPS * VIDEO_CHUNKS_PER_FRAME,
                (TOTAL_CLIENTS * TARGET_FPS * VIDEO_CHUNKS_PER_FRAME * VIDEO_CHUNK_SIZE * 8.0) / (1024 * 1024));
        System.out.printf("Theoretical Outbound Video: %,d packets/sec (%,.2f Mbps out of server)%n",
                TOTAL_CLIENTS * (CLIENTS_PER_SESSION - 1) * TARGET_FPS * VIDEO_CHUNKS_PER_FRAME,
                (TOTAL_CLIENTS * (CLIENTS_PER_SESSION - 1) * TARGET_FPS * VIDEO_CHUNKS_PER_FRAME * VIDEO_CHUNK_SIZE * 8.0) / (1024 * 1024));
        System.out.println("Duration                 : " + SIMULATION_DURATION_SEC + " seconds");
        System.out.println("=========================================================================================\n");

        InetSocketAddress serverAddress = new InetSocketAddress(HOST, PORT);
        List<SimulatedParticipant> participants = new ArrayList<>(TOTAL_CLIENTS);
        CountDownLatch mediaJoinLatch = new CountDownLatch(TOTAL_CLIENTS);
        CountDownLatch chatJoinLatch = new CountDownLatch(TOTAL_CLIENTS);

        // 1. Initialize All Participants with 2 Sessions each
        System.out.println("🔄 Initializing " + TOTAL_CLIENTS + " participants across " + NUM_SESSIONS + " group rooms (Preserved TCP + Unpreserved UDP)...");
        for (int s = 0; s < NUM_SESSIONS; s++) {
            String roomId = BASE_ROOM_ID + "-s" + s;
            String preservedChatSession = roomId + "-preserved-chat";
            String unpreservedMediaSession = roomId + "-unpreserved-media";

            for (int c = 0; c < CLIENTS_PER_SESSION; c++) {
                String userId = "Room" + s + "-User" + (c + 1);
                SimulatedParticipant p = new SimulatedParticipant(
                        userId,
                        unpreservedMediaSession,
                        preservedChatSession,
                        serverAddress,
                        mediaJoinLatch,
                        chatJoinLatch
                );
                participants.add(p);
                p.startReceivers();
            }
        }

        // 2. Authenticate into both sessions (HMAC JOIN_SESSION)
        System.out.println("🔑 Authenticating participants into Preserved (TCP) & Unpreserved (UDP) sessions...");
        for (SimulatedParticipant p : participants) {
            p.sendJoin();
        }

        // Wait for all clients to receive ACK on both transports
        boolean mediaJoined = mediaJoinLatch.await(15, TimeUnit.SECONDS);
        boolean chatJoined = chatJoinLatch.await(15, TimeUnit.SECONDS);

        if (!mediaJoined || !chatJoined) {
            System.err.println("⚠️ Warning: Latches timed out - Media ACKed: " + (TOTAL_CLIENTS - mediaJoinLatch.getCount())
                    + "/" + TOTAL_CLIENTS + ", Chat ACKed: " + (TOTAL_CLIENTS - chatJoinLatch.getCount()) + "/" + TOTAL_CLIENTS);
        } else {
            System.out.println("✅ All " + TOTAL_CLIENTS + " participants across " + NUM_SESSIONS + " rooms joined Preserved & Unpreserved sessions successfully!\n");
        }

        // 3. Unleash Media (UDP Audio+Video) & Preserved Chat (TCP)
        System.out.println("🚀 UNLEASHING REAL-TIME AUDIO/VIDEO (UDP) & PRESERVED CHAT (TCP) for " + SIMULATION_DURATION_SEC + " seconds...\n");
        long startTime = System.currentTimeMillis();

        // Start unpreserved media publishers (UDP: Audio + Video)
        for (SimulatedParticipant p : participants) {
            p.startMediaPublishing();
        }

        // Start preserved chat scheduler: 4 chats per second over reliable TCP
        ScheduledExecutorService chatScheduler = Executors.newScheduledThreadPool(2);
        chatScheduler.scheduleAtFixedRate(() -> {
            if (!running.get()) return;
            int idx = ThreadLocalRandom.current().nextInt(TOTAL_CLIENTS);
            SimulatedParticipant sender = participants.get(idx);
            sender.sendChatMessage("Preserved message from " + sender.userId + " [Time: " + System.currentTimeMillis() + "]");
        }, 100, 250, TimeUnit.MILLISECONDS);

        // 4. Live Telemetry Ticker (Every 1 second)
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
        AtomicLong lastMediaBytes = new AtomicLong(0);
        AtomicLong lastVideoPkts = new AtomicLong(0);
        AtomicLong lastAudioPkts = new AtomicLong(0);

        ticker.scheduleAtFixedRate(() -> {
            long currentBytes = mediaBytesReceived.get();
            long currentVideo = videoChunksReceived.get();
            long currentAudio = audioPktsReceived.get();

            long deltaBytes = currentBytes - lastMediaBytes.getAndSet(currentBytes);
            long deltaVideo = currentVideo - lastVideoPkts.getAndSet(currentVideo);
            long deltaAudio = currentAudio - lastAudioPkts.getAndSet(currentAudio);

            double mbps = (deltaBytes * 8.0) / (1024 * 1024);
            long chatRecv = chatReceived.get();

            System.out.printf("📊 [LIVE TICKER] Video: %,d pkts/s | Audio: %,d pkts/s | Bitrate: %.2f Mbps | TCP Preserved Chat Delivered: %,d msgs%n",
                    deltaVideo, deltaAudio, mbps, chatRecv);
        }, 1, 1, TimeUnit.SECONDS);

        // Run for benchmark duration
        Thread.sleep(SIMULATION_DURATION_SEC * 1000L);

        // 5. Shutdown and Collect Summary
        running.set(false);
        ticker.shutdown();
        chatScheduler.shutdown();

        // Allow 1.5 seconds for inflight packets to settle
        Thread.sleep(1500);

        // Gracefully leave room on server before closing sockets
        for (SimulatedParticipant p : participants) {
            p.close();
        }

        long totalDurationMs = System.currentTimeMillis() - startTime;
        printBenchmarkSummary(totalDurationMs);
    }

    private static void printBenchmarkSummary(long durationMs) {
        long totalVideoSent = videoChunksSent.get();
        long totalVideoRecv = videoChunksReceived.get();
        long totalAudioSent = audioPktsSent.get();
        long totalAudioRecv = audioPktsReceived.get();
        long totalBytes = mediaBytesReceived.get();
        long totalChatSent = chatSent.get();
        long totalChatRecv = chatReceived.get();

        // In 4-client group rooms, each broadcast reaches (CLIENTS_PER_SESSION - 1) = 3 peers
        long expectedVideoRecv = totalVideoSent * (CLIENTS_PER_SESSION - 1);
        double videoDeliveryRate = expectedVideoRecv > 0 ? ((double) totalVideoRecv / expectedVideoRecv) * 100.0 : 100.0;

        long expectedAudioRecv = totalAudioSent * (CLIENTS_PER_SESSION - 1);
        double audioDeliveryRate = expectedAudioRecv > 0 ? ((double) totalAudioRecv / expectedAudioRecv) * 100.0 : 100.0;

        long expectedChatRecv = totalChatSent * (CLIENTS_PER_SESSION - 1);
        double chatDeliveryRate = expectedChatRecv > 0 ? ((double) totalChatRecv / expectedChatRecv) * 100.0 : 100.0;

        double mbps = (totalBytes * 8.0) / (durationMs / 1000.0) / (1024 * 1024);

        List<Long> videoLatencies = new ArrayList<>(videoLatencySamples);
        Collections.sort(videoLatencies);
        long vP50 = videoLatencies.isEmpty() ? 0 : videoLatencies.get((int) (videoLatencies.size() * 0.50));
        long vP95 = videoLatencies.isEmpty() ? 0 : videoLatencies.get((int) (videoLatencies.size() * 0.95));
        long vMax = videoLatencies.isEmpty() ? 0 : videoLatencies.get(videoLatencies.size() - 1);

        List<Long> audioLatencies = new ArrayList<>(audioLatencySamples);
        Collections.sort(audioLatencies);
        long aP50 = audioLatencies.isEmpty() ? 0 : audioLatencies.get((int) (audioLatencies.size() * 0.50));
        long aP95 = audioLatencies.isEmpty() ? 0 : audioLatencies.get((int) (audioLatencies.size() * 0.95));

        System.out.println("\n=========================================================================================");
        System.out.println("📊 FINAL BENCHMARK SUMMARY (DUAL-SESSION 4-CLIENT GROUP ARCHITECTURE - 27 SESSIONS)");
        System.out.println("=========================================================================================");
        System.out.printf("Duration                          : %.2f seconds%n", durationMs / 1000.0);
        System.out.printf("Rooms / Participants             : %d rooms (%d participants total)%n", NUM_SESSIONS, TOTAL_CLIENTS);
        System.out.printf("Total Media Data Shuffled (UDP)   : %,.2f MB (%,.2f GB)%n",
                totalBytes / (1024.0 * 1024.0), totalBytes / (1024.0 * 1024.0 * 1024.0));
        System.out.printf("Aggregate Network Bandwidth       : %.2f Mbps%n", mbps);
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println("📹 UNPRESERVED VIDEO STREAM (UDP - Tag 0x02):");
        System.out.printf("  • Video Chunks Sent / Recv      : %,d / %,d (Delivery: %.2f%% - 1-to-3 Mesh)%n", totalVideoSent, totalVideoRecv, videoDeliveryRate);
        System.out.printf("  • Video Latency (p50 / p95 / max): %d ms / %d ms / %d ms%n", vP50, vP95, vMax);
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println("🎙️ UNPRESERVED AUDIO STREAM (UDP - Tag 0x01):");
        System.out.printf("  • Audio Packets Sent / Recv     : %,d / %,d (Delivery: %.2f%% - 1-to-3 Mesh)%n", totalAudioSent, totalAudioRecv, audioDeliveryRate);
        System.out.printf("  • Audio Latency (p50 / p95)     : %d ms / %d ms%n", aP50, aP95);
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println("💬 PRESERVED CHAT STREAM (TCP - Length-Prefixed Frames):");
        System.out.printf("  • Chat Messages Sent / Recv     : %,d / %,d (Delivery: %.2f%% - 100%% Guaranteed)%n", totalChatSent, totalChatRecv, chatDeliveryRate);
        System.out.println("=========================================================================================");
    }

    /**
     * Simulated Meeting Participant managing:
     * - Unpreserved UDP Datagrams for multiplexed Audio & Video
     * - Preserved TCP Stream for reliable persistent chat
     */
    static class SimulatedParticipant {
        final String userId;
        final String unpreservedMediaSession;
        final String preservedChatSession;
        final InetSocketAddress serverAddress;
        final CountDownLatch mediaJoinLatch;
        final CountDownLatch chatJoinLatch;

        final DatagramSocket udpSocket;
        final Socket tcpSocket;
        final DataOutputStream tcpOut;
        final DataInputStream tcpIn;

        final byte[] dummyAudioPayload = new byte[AUDIO_PACKET_SIZE];
        final byte[] dummyVideoPayload = new byte[VIDEO_CHUNK_SIZE];

        SimulatedParticipant(String userId, String unpreservedMediaSession, String preservedChatSession,
                             InetSocketAddress serverAddress,
                             CountDownLatch mediaJoinLatch, CountDownLatch chatJoinLatch) throws Exception {
            this.userId = userId;
            this.unpreservedMediaSession = unpreservedMediaSession;
            this.preservedChatSession = preservedChatSession;
            this.serverAddress = serverAddress;
            this.mediaJoinLatch = mediaJoinLatch;
            this.chatJoinLatch = chatJoinLatch;

            // 1. Dedicated UDP socket for Unpreserved Real-Time Media (Audio + Video)
            this.udpSocket = new DatagramSocket();
            this.udpSocket.setReceiveBufferSize(8 * 1024 * 1024);
            this.udpSocket.setSendBufferSize(1024 * 1024);

            // 2. Dedicated TCP socket for Preserved Chat (TCP_NODELAY enabled)
            this.tcpSocket = new Socket(serverAddress.getAddress(), serverAddress.getPort());
            this.tcpSocket.setTcpNoDelay(true);
            this.tcpSocket.setReceiveBufferSize(1024 * 1024);
            this.tcpSocket.setSendBufferSize(1024 * 1024);
            this.tcpOut = new DataOutputStream(new BufferedOutputStream(tcpSocket.getOutputStream(), 16384));
            this.tcpIn = new DataInputStream(new BufferedInputStream(tcpSocket.getInputStream(), 16384));

            // Fill dummy payloads
            ThreadLocalRandom.current().nextBytes(dummyAudioPayload);
            ThreadLocalRandom.current().nextBytes(dummyVideoPayload);
        }

        void sendJoin() {
            try {
                // Join Unpreserved Media Session over UDP
                String mediaToken = HmacTokenAuthorizer.generateToken(unpreservedMediaSession, userId, 3600, HMAC_SECRET);
                Packet mediaJoin = new Packet(
                        Packet.CURRENT_VERSION,
                        Opcode.JOIN_SESSION,
                        Packet.FLAG_NONE,
                        0,
                        (short) 0,
                        (short) 1,
                        unpreservedMediaSession,
                        null,
                        mediaToken.getBytes(StandardCharsets.UTF_8)
                );
                sendUdpPacket(mediaJoin);

                // Join Preserved Chat Session over TCP
                String chatToken = HmacTokenAuthorizer.generateToken(preservedChatSession, userId, 3600, HMAC_SECRET);
                Packet chatJoin = new Packet(
                        Packet.CURRENT_VERSION,
                        Opcode.JOIN_SESSION,
                        Packet.FLAG_NONE,
                        0,
                        (short) 0,
                        (short) 1,
                        preservedChatSession,
                        null,
                        chatToken.getBytes(StandardCharsets.UTF_8)
                );
                sendTcpPacket(chatJoin);

            } catch (Exception e) {
                System.err.println("Join failed for " + userId + ": " + e.getMessage());
            }
        }

        void sendLeave() {
            try {
                Packet mediaLeave = new Packet(
                        Packet.CURRENT_VERSION,
                        Opcode.LEAVE_SESSION,
                        Packet.FLAG_NONE,
                        0,
                        (short) 0,
                        (short) 1,
                        unpreservedMediaSession,
                        null,
                        new byte[0]
                );
                sendUdpPacket(mediaLeave);

                Packet chatLeave = new Packet(
                        Packet.CURRENT_VERSION,
                        Opcode.LEAVE_SESSION,
                        Packet.FLAG_NONE,
                        0,
                        (short) 0,
                        (short) 1,
                        preservedChatSession,
                        null,
                        new byte[0]
                );
                sendTcpPacket(chatLeave);
            } catch (Exception ignored) {
            }
        }

        void startReceivers() {
            startUdpMediaReceiver();
            startTcpChatReceiver();
        }

        /**
         * Dedicated high-speed UDP receiver for multiplexed Audio (0x01) and Video (0x02) packets.
         */
        private void startUdpMediaReceiver() {
            Thread.ofVirtual().name("rx-media-" + userId).start(() -> {
                byte[] buf = new byte[65535];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                while (running.get() && !udpSocket.isClosed()) {
                    try {
                        // CRITICAL: Reset packet length before every receive call so DatagramSocket doesn't truncate packets!
                        packet.setData(buf, 0, buf.length);
                        udpSocket.receive(packet);
                        long recvTime = System.currentTimeMillis();
                        int len = packet.getLength();

                        // Fast zero-allocation header inspection
                        if (len < BinaryPacketCodec.MIN_PACKET_HEADER_SIZE) continue;
                        if (buf[0] != Packet.MAGIC_BYTE) continue;

                        byte opcode = buf[2];
                        if (opcode == Opcode.ACK.getCode()) {
                            mediaJoinLatch.countDown();
                            continue;
                        }

                        if (opcode == Opcode.BROADCAST.getCode() || opcode == Opcode.BROADCAST_ALL.getCode()) {
                            byte flags = buf[3];
                            int sessionLen = Byte.toUnsignedInt(buf[12]);
                            int payloadHeaderOffset = 13 + sessionLen;

                            boolean isDirect = (flags & Packet.FLAG_DIRECT) != 0;
                            if (isDirect && len > payloadHeaderOffset) {
                                int targetLen = Byte.toUnsignedInt(buf[payloadHeaderOffset]);
                                payloadHeaderOffset += 1 + targetLen;
                            }

                            if (len < payloadHeaderOffset + 4) continue;

                            int payloadLen = ((buf[payloadHeaderOffset] & 0xFF) << 24)
                                    | ((buf[payloadHeaderOffset + 1] & 0xFF) << 16)
                                    | ((buf[payloadHeaderOffset + 2] & 0xFF) << 8)
                                    | (buf[payloadHeaderOffset + 3] & 0xFF);

                            int payloadStart = payloadHeaderOffset + 4;
                            if (payloadLen > 9 && len >= payloadStart + 9) {
                                byte tag = buf[payloadStart]; // 0x01: Audio, 0x02: Video
                                mediaBytesReceived.addAndGet(payloadLen);

                                long sentTime = ((long) (buf[payloadStart + 1] & 0xFF) << 56)
                                        | ((long) (buf[payloadStart + 2] & 0xFF) << 48)
                                        | ((long) (buf[payloadStart + 3] & 0xFF) << 40)
                                        | ((long) (buf[payloadStart + 4] & 0xFF) << 32)
                                        | ((long) (buf[payloadStart + 5] & 0xFF) << 24)
                                        | ((long) (buf[payloadStart + 6] & 0xFF) << 16)
                                        | ((long) (buf[payloadStart + 7] & 0xFF) << 8)
                                        | ((long) (buf[payloadStart + 8] & 0xFF));

                                long latency = Math.max(0, recvTime - sentTime);

                                if (tag == TAG_VIDEO) {
                                    videoChunksReceived.incrementAndGet();
                                    if ((videoChunksReceived.get() & 127) == 0) {
                                        videoLatencySamples.add(latency);
                                    }
                                } else if (tag == TAG_AUDIO) {
                                    audioPktsReceived.incrementAndGet();
                                    if ((audioPktsReceived.get() & 127) == 0) {
                                        audioLatencySamples.add(latency);
                                    }
                                }
                            }
                        }
                    } catch (Exception ignored) {
                        break;
                    }
                }
            });
        }

        /**
         * Dedicated reliable TCP receiver virtual thread for incoming preserved chat messages.
         */
        private void startTcpChatReceiver() {
            Thread.ofVirtual().name("rx-chat-" + userId).start(() -> {
                try {
                    while (running.get() && !tcpSocket.isClosed()) {
                        int frameLength = tcpIn.readInt();
                        if (frameLength <= 0 || frameLength > 16 * 1024 * 1024) break;

                        byte[] frame = tcpIn.readNBytes(frameLength);
                        if (frame.length < BinaryPacketCodec.MIN_PACKET_HEADER_SIZE) continue;
                        if (frame[0] != Packet.MAGIC_BYTE) continue;

                        byte opcode = frame[2];
                        if (opcode == Opcode.ACK.getCode()) {
                            chatJoinLatch.countDown();
                            continue;
                        }

                        if (opcode == Opcode.BROADCAST.getCode() || opcode == Opcode.BROADCAST_ALL.getCode()) {
                            chatReceived.incrementAndGet();
                        }
                    }
                } catch (Exception ignored) {
                }
            });
        }

        /**
         * Starts publishing both Audio (50 pkts/s) and Video (30 FPS) over unpreserved UDP.
         */
        void startMediaPublishing() {
            // 1. Start Audio Publisher Thread (Opus Voice: Every 20ms = 50 pkts/sec)
            Thread.ofVirtual().name("tx-audio-" + userId).start(() -> {
                while (running.get() && !udpSocket.isClosed()) {
                    try {
                        long audioTime = System.currentTimeMillis();
                        ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + dummyAudioPayload.length);
                        buffer.put(TAG_AUDIO); // Tag 0x01
                        buffer.putLong(audioTime); // Timestamp for lip-sync & latency
                        buffer.put(dummyAudioPayload);

                        Packet audioPacket = Packet.ofBroadcast(unpreservedMediaSession, buffer.array());
                        sendUdpPacket(audioPacket);
                        audioPktsSent.incrementAndGet();

                        Thread.sleep(20); // 20 ms Opus voice cadence
                    } catch (Exception ignored) {
                    }
                }
            });

            // 2. Start Video Publisher Thread (30 FPS 1080p chunks)
            Thread.ofVirtual().name("tx-video-" + userId).start(() -> {
                int frameNumber = 0;
                long nextFrameTime = System.nanoTime();

                while (running.get() && !udpSocket.isClosed()) {
                    try {
                        long frameStart = System.currentTimeMillis();
                        frameNumber++;

                        // Send MTU chunks per frame over UDP
                        for (short chunkIdx = 0; chunkIdx < VIDEO_CHUNKS_PER_FRAME; chunkIdx++) {
                            ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + 4 + dummyVideoPayload.length);
                            buffer.put(TAG_VIDEO); // Tag 0x02
                            buffer.putLong(frameStart); // Sent timestamp
                            buffer.putInt(frameNumber); // Frame number
                            buffer.put(dummyVideoPayload);

                            Packet chunkPacket = Packet.ofChunk(
                                    Opcode.BROADCAST,
                                    unpreservedMediaSession,
                                    null,
                                    frameNumber,
                                    chunkIdx,
                                    (short) VIDEO_CHUNKS_PER_FRAME,
                                    buffer.array()
                            );

                            sendUdpPacket(chunkPacket);
                            videoChunksSent.incrementAndGet();
                        }

                        // High precision pacing
                        nextFrameTime += VIDEO_FRAME_DURATION_NANOS;
                        long sleepNanos = nextFrameTime - System.nanoTime();
                        if (sleepNanos > 2_000_000) {
                            LockSupport.parkNanos(sleepNanos - 1_000_000);
                        }
                        while (System.nanoTime() < nextFrameTime) {
                            Thread.onSpinWait();
                        }

                    } catch (Exception ignored) {
                    }
                }
            });
        }

        /**
         * Delivers chat messages exclusively through reliable preserved TCP streams.
         */
        void sendChatMessage(String text) {
            if (tcpSocket.isClosed()) return;
            try {
                byte[] payload = text.getBytes(StandardCharsets.UTF_8);
                Packet chatPacket = Packet.ofBroadcast(preservedChatSession, payload);
                sendTcpPacket(chatPacket);
                chatSent.incrementAndGet();
            } catch (Exception ignored) {
            }
        }

        private void sendUdpPacket(Packet p) throws Exception {
            byte[] bytes = CODEC.encode(p);
            DatagramPacket dp = new DatagramPacket(bytes, bytes.length, serverAddress);
            udpSocket.send(dp);
        }

        private synchronized void sendTcpPacket(Packet p) throws Exception {
            byte[] encoded = CODEC.encode(p);
            tcpOut.writeInt(encoded.length);
            tcpOut.write(encoded);
            tcpOut.flush();
        }

        void close() {
            try {
                sendLeave();
                udpSocket.close();
                tcpSocket.close();
            } catch (Exception ignored) {
            }
        }
    }
}

package com.anilyanai.gamingsimulation;

import com.anilyanai.business.security.HmacTokenAuthorizer;
import com.anilyanai.protocol.Opcode;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.BinaryPacketCodec;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
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
 * 🎮 8-PLAYER ARENA / TACTICAL FPS PURE UDP BENCHMARK (20 SESSIONS x 8 PLAYERS = 160 PLAYERS)
 * =========================================================================================
 *
 * <p>Simulates competitive multiplayer FPS games (e.g., CS2 / Valorant / Overwatch 2) over Pure UDP:
 * <ul>
 *   <li><b>20 Independent Match Sessions:</b> 8 players per match (4v4 arena battle, 160 total players).</li>
 *   <li><b>Transport:</b> 100% Pure UDP (Zero TCP overhead).</li>
 *   <li><b>Tick Rate:</b> 64 Hz (64 input/movement packets per second per player, ~15.6 ms interval).</li>
 *   <li><b>Input Payload Structure (~64 bytes):</b> Sequence number, timestamp, input bitmask (WASD/Jump/Shoot),
 *       view angles (pitch/yaw), position (X, Y, Z), velocity vector.</li>
 *   <li><b>1-to-7 Mesh Fan-Out:</b> Each player's input snapshot is broadcast to the other 7 players in the match.</li>
 * </ul>
 * </p>
 */
public class EightClientGamingSimulationBenchmark {

    // Network & Session Base Config
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 8080;
    private static final String BASE_MATCH_ID = "arena8-match-" + (System.currentTimeMillis() % 1_000_000);
    private static final String HMAC_SECRET = "collab-benchmark-secret-key-256bit-ok";

    // 8-Player Arena Topology: 20 matches x 8 players = 160 players
    public static final int NUM_SESSIONS = 20;
    public static final int CLIENTS_PER_SESSION = 8; // Exactly 8 players per match (4v4)
    public static final int TOTAL_PLAYERS = NUM_SESSIONS * CLIENTS_PER_SESSION;
    public static final int TICK_RATE_HZ = 64; // 64 Hz competitive tick rate
    public static final long TICK_INTERVAL_NANOS = 1_000_000_000L / TICK_RATE_HZ; // ~15.625 ms
    public static final int SIMULATION_DURATION_SEC = 10; // Run for 10 seconds

    // Payload Stream Tags
    private static final byte TAG_GAME_TICK = 0x10; // Unpreserved 64 Hz player input / physics tick

    // Codec & State
    private static final BinaryPacketCodec CODEC = new BinaryPacketCodec();
    private static final AtomicBoolean running = new AtomicBoolean(true);

    // Global Telemetry Metrics
    private static final AtomicLong ticksSent = new AtomicLong(0);
    private static final AtomicLong ticksReceived = new AtomicLong(0);
    private static final AtomicLong totalBytesReceived = new AtomicLong(0);
    private static final ConcurrentLinkedQueue<Long> tickLatencySamples = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws Exception {
        System.out.println("=========================================================================================");
        System.out.println("🎮 8-PLAYER ARENA PURE UDP BENCHMARK (" + NUM_SESSIONS + " MATCHES x 8 PLAYERS = " + TOTAL_PLAYERS + " TOTAL PLAYERS)");
        System.out.println("=========================================================================================");
        System.out.println("Target Server            : " + HOST + ":" + PORT + " (100% PURE UDP)");
        System.out.println("Total Match Rooms        : " + NUM_SESSIONS + " matches (4v4 Arena)");
        System.out.println("Players per Match        : " + CLIENTS_PER_SESSION + " players");
        System.out.println("Total Active Players     : " + TOTAL_PLAYERS);
        System.out.println("Simulation Tick Rate     : " + TICK_RATE_HZ + " Hz (~15.6 ms per tick)");
        System.out.println("Transport                : Pure UDP (No TCP / No preserved session)");
        System.out.printf("Inbound Player Ticks     : %,d packets/sec (%,.2f Mbps into server)%n",
                TOTAL_PLAYERS * TICK_RATE_HZ,
                (TOTAL_PLAYERS * TICK_RATE_HZ * 64 * 8.0) / (1024 * 1024));
        System.out.printf("Theoretical Outbound Mesh: %,d packets/sec (%,.2f Mbps out of server - %d fanout streams)%n",
                TOTAL_PLAYERS * (CLIENTS_PER_SESSION - 1) * TICK_RATE_HZ,
                (TOTAL_PLAYERS * (CLIENTS_PER_SESSION - 1) * TICK_RATE_HZ * 64 * 8.0) / (1024 * 1024),
                NUM_SESSIONS * CLIENTS_PER_SESSION * (CLIENTS_PER_SESSION - 1));
        System.out.println("Duration                 : " + SIMULATION_DURATION_SEC + " seconds");
        System.out.println("=========================================================================================\n");

        InetSocketAddress serverAddress = new InetSocketAddress(HOST, PORT);
        List<SimulatedPlayer> players = new ArrayList<>(TOTAL_PLAYERS);
        CountDownLatch tickJoinLatch = new CountDownLatch(TOTAL_PLAYERS);

        // 1. Initialize All Players (Pure UDP)
        System.out.println("🔄 Initializing " + TOTAL_PLAYERS + " players across " + NUM_SESSIONS + " matches over Pure UDP...");
        for (int m = 0; m < NUM_SESSIONS; m++) {
            String matchId = BASE_MATCH_ID + "-m" + m;
            String unpreservedTickSession = matchId + "-udp-arena";

            for (int p = 0; p < CLIENTS_PER_SESSION; p++) {
                String playerId = "Match" + m + "-P" + (p + 1);
                SimulatedPlayer player = new SimulatedPlayer(
                        playerId,
                        unpreservedTickSession,
                        serverAddress,
                        tickJoinLatch
                );
                players.add(player);
                player.startReceiver();
            }
        }

        // 2. Authenticate into UDP session (HMAC JOIN_SESSION)
        System.out.println("🔑 Authenticating players into UDP match sessions...");
        for (SimulatedPlayer player : players) {
            player.sendJoin();
        }

        // Wait for all clients to receive ACK
        boolean ticksJoined = tickJoinLatch.await(15, TimeUnit.SECONDS);

        if (!ticksJoined) {
            System.err.println("⚠️ Warning: UDP Join Latches timed out - ACKed: "
                    + (TOTAL_PLAYERS - tickJoinLatch.getCount()) + "/" + TOTAL_PLAYERS);
        } else {
            System.out.println("✅ All " + TOTAL_PLAYERS + " players joined UDP match sessions successfully!\n");
        }

        // 3. Unleash 64 Hz Gaming Ticks
        System.out.println("🚀 UNLEASHING 64 Hz PURE UDP GAMING TICKS for " + SIMULATION_DURATION_SEC + " seconds...\n");
        long startTime = System.currentTimeMillis();

        // Start 64 Hz game loop threads for all players
        for (SimulatedPlayer player : players) {
            player.startGameLoop();
        }

        // 4. Live Telemetry Ticker (Every 1 second)
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
        AtomicLong lastBytes = new AtomicLong(0);
        AtomicLong lastTicks = new AtomicLong(0);

        ticker.scheduleAtFixedRate(() -> {
            long currentBytes = totalBytesReceived.get();
            long currentTicks = ticksReceived.get();

            long deltaBytes = currentBytes - lastBytes.getAndSet(currentBytes);
            long deltaTicks = currentTicks - lastTicks.getAndSet(currentTicks);

            double mbps = (deltaBytes * 8.0) / (1024 * 1024);

            System.out.printf("📊 [LIVE TICKER] Ingested Ticks: %,d pkts/s | Server Egress: %.2f Mbps%n",
                    deltaTicks, mbps);
        }, 1, 1, TimeUnit.SECONDS);

        // Run for benchmark duration
        Thread.sleep(SIMULATION_DURATION_SEC * 1000L);

        // 5. Shutdown and Collect Summary
        running.set(false);
        ticker.shutdown();

        // Allow 1.5 seconds for inflight packets to settle
        Thread.sleep(1500);

        for (SimulatedPlayer player : players) {
            player.close();
        }

        long totalDurationMs = System.currentTimeMillis() - startTime;
        printBenchmarkSummary(totalDurationMs);
    }

    private static void printBenchmarkSummary(long durationMs) {
        long totalTicksSent = ticksSent.get();
        long totalTicksRecv = ticksReceived.get();
        long totalBytes = totalBytesReceived.get();

        // In 8-player matches, each broadcast reaches (CLIENTS_PER_SESSION - 1) = 7 peers
        long expectedTicksRecv = totalTicksSent * (CLIENTS_PER_SESSION - 1);
        double tickDeliveryRate = expectedTicksRecv > 0 ? ((double) totalTicksRecv / expectedTicksRecv) * 100.0 : 100.0;

        double mbps = (totalBytes * 8.0) / (durationMs / 1000.0) / (1024 * 1024);

        List<Long> latencies = new ArrayList<>(tickLatencySamples);
        Collections.sort(latencies);
        long p50 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.50));
        long p95 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.95));
        long p99 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.99));
        long max = latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1);

        System.out.println("\n=========================================================================================");
        System.out.println("📊 FINAL BENCHMARK SUMMARY (8-PLAYER ARENA 64 Hz GAMING ENGINE - PURE UDP)");
        System.out.println("=========================================================================================");
        System.out.printf("Duration                          : %.2f seconds%n", durationMs / 1000.0);
        System.out.printf("Matches / Active Players          : %d matches (%d players total)%n", NUM_SESSIONS, TOTAL_PLAYERS);
        System.out.printf("Total Game Data Shuffled (UDP)    : %,.2f MB%n", totalBytes / (1024.0 * 1024.0));
        System.out.printf("Aggregate Network Bandwidth       : %.2f Mbps%n", mbps);
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println("🎯 64 Hz GAME STATE TICKS (PURE UDP):");
        System.out.printf("  • Player Ticks Sent / Recv      : %,d / %,d (Delivery: %.2f%% - 1-to-7 Mesh)%n", totalTicksSent, totalTicksRecv, tickDeliveryRate);
        System.out.printf("  • Tick Latency (p50 / p95 / p99 / max): %d ms / %d ms / %d ms / %d ms%n", p50, p95, p99, max);
        System.out.println("=========================================================================================");
    }

    /**
     * Simulated Multiplayer Player Client managing:
     * - 64 Hz Game State Input & Delta Packets over Pure UDP
     */
    static class SimulatedPlayer {
        final String playerId;
        final String unpreservedTickSession;
        final InetSocketAddress serverAddress;
        final CountDownLatch tickJoinLatch;

        final DatagramSocket udpSocket;

        // Simulated in-game physics & coordinates
        private float posX = 100.0f, posY = 0.0f, posZ = 50.0f;
        private float pitch = 0.0f, yaw = 90.0f;
        private int sequenceNumber = 0;

        SimulatedPlayer(String playerId, String unpreservedTickSession,
                        InetSocketAddress serverAddress,
                        CountDownLatch tickJoinLatch) throws Exception {
            this.playerId = playerId;
            this.unpreservedTickSession = unpreservedTickSession;
            this.serverAddress = serverAddress;
            this.tickJoinLatch = tickJoinLatch;

            // Dedicated UDP socket for ultra-fast 64 Hz Game Ticks
            this.udpSocket = new DatagramSocket();
            this.udpSocket.setReceiveBufferSize(4 * 1024 * 1024);
            this.udpSocket.setSendBufferSize(1024 * 1024);
        }

        void sendJoin() {
            try {
                // Join Unpreserved Tick Session over UDP
                String tickToken = HmacTokenAuthorizer.generateToken(unpreservedTickSession, playerId, 3600, HMAC_SECRET);
                Packet tickJoin = new Packet(
                        Packet.CURRENT_VERSION,
                        Opcode.JOIN_SESSION,
                        Packet.FLAG_NONE,
                        0,
                        (short) 0,
                        (short) 1,
                        unpreservedTickSession,
                        null,
                        tickToken.getBytes(StandardCharsets.UTF_8)
                );
                sendUdpPacket(tickJoin);
            } catch (Exception e) {
                System.err.println("Join failed for " + playerId + ": " + e.getMessage());
            }
        }

        void sendLeave() {
            try {
                Packet tickLeave = new Packet(
                        Packet.CURRENT_VERSION,
                        Opcode.LEAVE_SESSION,
                        Packet.FLAG_NONE,
                        0,
                        (short) 0,
                        (short) 1,
                        unpreservedTickSession,
                        null,
                        new byte[0]
                );
                sendUdpPacket(tickLeave);
            } catch (Exception ignored) {
            }
        }

        void startReceiver() {
            Thread.ofVirtual().name("rx-tick-" + playerId).start(() -> {
                byte[] buf = new byte[65535];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                while (running.get() && !udpSocket.isClosed()) {
                    try {
                        // CRITICAL: Reset packet length before every receive call so DatagramSocket doesn't truncate packets!
                        packet.setData(buf, 0, buf.length);
                        udpSocket.receive(packet);
                        long recvTime = System.currentTimeMillis();
                        int len = packet.getLength();

                        if (len < BinaryPacketCodec.MIN_PACKET_HEADER_SIZE) continue;
                        if (buf[0] != Packet.MAGIC_BYTE) continue;

                        byte opcode = buf[2];
                        if (opcode == Opcode.ACK.getCode()) {
                            tickJoinLatch.countDown();
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
                                byte tag = buf[payloadStart]; // 0x10: GAME_TICK
                                if (tag == TAG_GAME_TICK) {
                                    totalBytesReceived.addAndGet(payloadLen);
                                    ticksReceived.incrementAndGet();

                                    long sentTime = ((long) (buf[payloadStart + 1] & 0xFF) << 56)
                                            | ((long) (buf[payloadStart + 2] & 0xFF) << 48)
                                            | ((long) (buf[payloadStart + 3] & 0xFF) << 40)
                                            | ((long) (buf[payloadStart + 4] & 0xFF) << 32)
                                            | ((long) (buf[payloadStart + 5] & 0xFF) << 24)
                                            | ((long) (buf[payloadStart + 6] & 0xFF) << 16)
                                            | ((long) (buf[payloadStart + 7] & 0xFF) << 8)
                                            | (buf[payloadStart + 8] & 0xFF);

                                    long latency = Math.max(0, recvTime - sentTime);
                                    if ((ticksReceived.get() & 127) == 0) {
                                        tickLatencySamples.add(latency);
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
         * Runs the high-precision 64 Hz Client Simulation Loop (~15.625 ms per tick).
         */
        void startGameLoop() {
            Thread.ofVirtual().name("tx-game-tick-" + playerId).start(() -> {
                long nextTickTime = System.nanoTime();

                while (running.get() && !udpSocket.isClosed()) {
                    try {
                        long tickStart = System.currentTimeMillis();
                        sequenceNumber++;

                        // Simulate player movement & mouse look
                        posX += ThreadLocalRandom.current().nextFloat(-0.5f, 0.5f);
                        posY += ThreadLocalRandom.current().nextFloat(-0.2f, 0.2f);
                        posZ += ThreadLocalRandom.current().nextFloat(-0.5f, 0.5f);
                        yaw += ThreadLocalRandom.current().nextFloat(-1.0f, 1.0f);

                        // Structure realistic 64-byte Game Input Packet
                        ByteBuffer buf = ByteBuffer.allocate(64);
                        buf.put(TAG_GAME_TICK);              // 1 byte: Stream Tag (0x10)
                        buf.putLong(tickStart);              // 8 bytes: Timestamp
                        buf.putInt(sequenceNumber);          // 4 bytes: Sequence #
                        buf.putShort((short) 0b00000001001); // 2 bytes: Input bitmask (W + Sprint + Fire)
                        buf.putFloat(posX);                  // 4 bytes: Pos X
                        buf.putFloat(posY);                  // 4 bytes: Pos Y
                        buf.putFloat(posZ);                  // 4 bytes: Pos Z
                        buf.putFloat(pitch);                 // 4 bytes: View Pitch
                        buf.putFloat(yaw);                   // 4 bytes: View Yaw
                        buf.putFloat(5.5f);                  // 4 bytes: Velocity X
                        buf.putFloat(0.0f);                  // 4 bytes: Velocity Y
                        buf.putFloat(3.2f);                  // 4 bytes: Velocity Z
                        buf.put((byte) 100);                 // 1 byte: Health
                        buf.put((byte) 50);                  // 1 byte: Armor
                        buf.put((byte) 1);                   // 1 byte: Selected Weapon Slot (AK-47)
                        buf.put(new byte[18]);               // 18 bytes: Sub-tick micro-inputs & padding = 64B total

                        Packet tickPacket = Packet.ofBroadcast(unpreservedTickSession, buf.array());
                        sendUdpPacket(tickPacket);
                        ticksSent.incrementAndGet();

                        // High precision 64 Hz pacing
                        nextTickTime += TICK_INTERVAL_NANOS;
                        long sleepNanos = nextTickTime - System.nanoTime();
                        if (sleepNanos > 2_000_000) {
                            LockSupport.parkNanos(sleepNanos - 1_000_000);
                        }
                        while (System.nanoTime() < nextTickTime) {
                            Thread.onSpinWait();
                        }

                    } catch (Exception ignored) {
                    }
                }
            });
        }

        private void sendUdpPacket(Packet p) throws Exception {
            byte[] bytes = CODEC.encode(p);
            DatagramPacket dp = new DatagramPacket(bytes, bytes.length, serverAddress);
            udpSocket.send(dp);
        }

        void close() {
            try {
                sendLeave();
                udpSocket.close();
            } catch (Exception ignored) {
            }
        }
    }
}

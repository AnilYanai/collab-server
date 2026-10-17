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
 * 🪂 100-PLAYER BATTLE ROYALE (PUBG-STYLE) PURE UDP GAMING BENCHMARK (1 MATCH x 100 PLAYERS)
 * =========================================================================================
 *
 * <p>Simulates a full 100-player Battle Royale match operating exclusively over ultra-low latency UDP:
 * <ul>
 *   <li><b>1 Match Session:</b> Exactly 100 players in a single match session (100 total active players).</li>
 *   <li><b>Transport:</b> 100% Pure UDP (Zero TCP overhead).</li>
 *   <li><b>Tick Rate:</b> 30 Hz (30 movement/input packets per second per player, ~33.3 ms interval).</li>
 *   <li><b>Player State Payload (~72 bytes):</b> Timestamp, sequence number, coordinates (X, Y, Z),
 *       heading, stance (standing/crouched/prone/driving/parachuting), health, equipped weapon, vehicle state.</li>
 *   <li><b>1-to-99 Massive Fan-Out:</b> Each player's tick is broadcast across all 99 opponents in the match (297,000 fanout pkts/sec).</li>
 * </ul>
 * </p>
 */
public class BattleRoyaleGamingSimulationBenchmark {

    // Network & Session Base Config
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 8080;
    private static final String BASE_MATCH_ID = "pubg-match-" + (System.currentTimeMillis() % 1_000_000);
    private static final String HMAC_SECRET = "collab-benchmark-secret-key-256bit-ok";

    // PUBG Topology: 1 match x 100 players = 100 total players (1-to-99 fanout)
    public static final int NUM_SESSIONS = 1;
    public static final int PLAYERS_PER_SESSION = 100; // 100-player Battle Royale island
    public static final int TOTAL_PLAYERS = NUM_SESSIONS * PLAYERS_PER_SESSION;
    public static final int TICK_RATE_HZ = 30; // 30 Hz standard BR tick rate
    public static final long TICK_INTERVAL_NANOS = 1_000_000_000L / TICK_RATE_HZ; // ~33.33 ms
    public static final int SIMULATION_DURATION_SEC = 10; // Run for 10 seconds

    // Payload Stream Tags
    private static final byte TAG_BR_TICK = 0x20; // Unpreserved 30 Hz BR player position/state

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
        System.out.println("🪂 100-PLAYER PUBG-STYLE PURE UDP BATTLE ROYALE BENCHMARK (1 MATCH x 100 PLAYERS)");
        System.out.println("=========================================================================================");
        System.out.println("Target Server            : " + HOST + ":" + PORT + " (100% PURE UDP)");
        System.out.println("Total Battle Royale Match: " + NUM_SESSIONS + " match (100-Player Island Match)");
        System.out.println("Players in Match         : " + PLAYERS_PER_SESSION + " players");
        System.out.println("Total Active Players     : " + TOTAL_PLAYERS);
        System.out.println("Simulation Tick Rate     : " + TICK_RATE_HZ + " Hz (~33.3 ms per tick)");
        System.out.println("Transport                : Pure UDP (No TCP / No preserved session)");
        System.out.printf("Inbound Player Ticks     : %,d packets/sec (%,.2f Mbps into server)%n",
                TOTAL_PLAYERS * TICK_RATE_HZ,
                (TOTAL_PLAYERS * TICK_RATE_HZ * 72 * 8.0) / (1024 * 1024));
        System.out.printf("Theoretical Outbound Mesh: %,d packets/sec (%,.2f Mbps out of server - %d fanout streams)%n",
                TOTAL_PLAYERS * (PLAYERS_PER_SESSION - 1) * TICK_RATE_HZ,
                (TOTAL_PLAYERS * (PLAYERS_PER_SESSION - 1) * TICK_RATE_HZ * 72 * 8.0) / (1024 * 1024),
                NUM_SESSIONS * PLAYERS_PER_SESSION * (PLAYERS_PER_SESSION - 1));
        System.out.println("Duration                 : " + SIMULATION_DURATION_SEC + " seconds");
        System.out.println("=========================================================================================\n");

        InetSocketAddress serverAddress = new InetSocketAddress(HOST, PORT);
        List<SimulatedBRPlayer> players = new ArrayList<>(TOTAL_PLAYERS);
        CountDownLatch tickJoinLatch = new CountDownLatch(TOTAL_PLAYERS);

        // 1. Initialize All 100 Players (Pure UDP)
        System.out.println("🔄 Initializing " + TOTAL_PLAYERS + " players in 100-player Battle Royale match over UDP...");
        for (int m = 0; m < NUM_SESSIONS; m++) {
            String matchId = BASE_MATCH_ID + "-m" + m;
            String unpreservedTickSession = matchId + "-udp-match";

            for (int p = 0; p < PLAYERS_PER_SESSION; p++) {
                String playerId = "PUBG_Player_" + (p + 1);
                SimulatedBRPlayer player = new SimulatedBRPlayer(
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
        System.out.println("🔑 Authenticating 100 players into UDP match session...");
        for (SimulatedBRPlayer player : players) {
            player.sendJoin();
        }

        // Wait for all clients to receive ACK
        boolean ticksJoined = tickJoinLatch.await(15, TimeUnit.SECONDS);

        if (!ticksJoined) {
            System.err.println("⚠️ Warning: UDP Join Latches timed out - ACKed: "
                    + (TOTAL_PLAYERS - tickJoinLatch.getCount()) + "/" + TOTAL_PLAYERS);
        } else {
            System.out.println("✅ All " + TOTAL_PLAYERS + " PUBG players joined UDP match successfully!\n");
        }

        // 3. Unleash 30 Hz Player Movement
        System.out.println("🚀 UNLEASHING 30 Hz PURE UDP BATTLE ROYALE TICKS for " + SIMULATION_DURATION_SEC + " seconds...\n");
        long startTime = System.currentTimeMillis();

        // Start 30 Hz movement loop threads for all players
        for (SimulatedBRPlayer player : players) {
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

            System.out.printf("📊 [LIVE TICKER] Ingested BR Ticks: %,d pkts/s | Server Egress: %.2f Mbps%n",
                    deltaTicks, mbps);
        }, 1, 1, TimeUnit.SECONDS);

        // Run for benchmark duration
        Thread.sleep(SIMULATION_DURATION_SEC * 1000L);

        // 5. Shutdown and Collect Summary
        running.set(false);
        ticker.shutdown();

        // Allow 1.5 seconds for inflight packets to settle
        Thread.sleep(1500);

        for (SimulatedBRPlayer player : players) {
            player.close();
        }

        long totalDurationMs = System.currentTimeMillis() - startTime;
        printBenchmarkSummary(totalDurationMs);
    }

    private static void printBenchmarkSummary(long durationMs) {
        long totalTicksSent = ticksSent.get();
        long totalTicksRecv = ticksReceived.get();
        long totalBytes = totalBytesReceived.get();

        // In 100-player lobbies, each broadcast reaches (PLAYERS_PER_SESSION - 1) = 99 peers
        long expectedTicksRecv = totalTicksSent * (PLAYERS_PER_SESSION - 1);
        double tickDeliveryRate = expectedTicksRecv > 0 ? ((double) totalTicksRecv / expectedTicksRecv) * 100.0 : 100.0;

        double mbps = (totalBytes * 8.0) / (durationMs / 1000.0) / (1024 * 1024);

        List<Long> latencies = new ArrayList<>(tickLatencySamples);
        Collections.sort(latencies);
        long p50 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.50));
        long p95 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.95));
        long p99 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.99));
        long max = latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1);

        System.out.println("\n=========================================================================================");
        System.out.println("📊 FINAL BENCHMARK SUMMARY (100-PLAYER PUBG-STYLE BATTLE ROYALE - PURE UDP)");
        System.out.println("=========================================================================================");
        System.out.printf("Duration                          : %.2f seconds%n", durationMs / 1000.0);
        System.out.printf("Matches / Active Players          : %d match (%d players total)%n", NUM_SESSIONS, TOTAL_PLAYERS);
        System.out.printf("Total Game Data Shuffled (UDP)    : %,.2f MB%n", totalBytes / (1024.0 * 1024.0));
        System.out.printf("Aggregate Network Bandwidth       : %.2f Mbps%n", mbps);
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println("🪂 30 Hz BATTLE ROYALE TICKS (PURE UDP):");
        System.out.printf("  • Player Ticks Sent / Recv      : %,d / %,d (Delivery: %.2f%% - 1-to-99 Lobby Fanout)%n", totalTicksSent, totalTicksRecv, tickDeliveryRate);
        System.out.printf("  • Tick Latency (p50 / p95 / p99 / max): %d ms / %d ms / %d ms / %d ms%n", p50, p95, p99, max);
        System.out.println("=========================================================================================");
    }

    /**
     * Simulated Battle Royale Player Client managing:
     * - 30 Hz Player Position & Vehicle State Packets over UDP
     */
    static class SimulatedBRPlayer {
        final String playerId;
        final String unpreservedTickSession;
        final InetSocketAddress serverAddress;
        final CountDownLatch tickJoinLatch;

        final DatagramSocket udpSocket;

        // Realistic 8x8 km Erangel coordinates & state
        private float posX = 4000.0f, posY = 150.0f, posZ = 3500.0f;
        private float heading = 180.0f;
        private int sequenceNumber = 0;

        SimulatedBRPlayer(String playerId, String unpreservedTickSession,
                          InetSocketAddress serverAddress,
                          CountDownLatch tickJoinLatch) throws Exception {
            this.playerId = playerId;
            this.unpreservedTickSession = unpreservedTickSession;
            this.serverAddress = serverAddress;
            this.tickJoinLatch = tickJoinLatch;

            // Dedicated UDP socket for 30 Hz high-volume BR ticks
            this.udpSocket = new DatagramSocket();
            this.udpSocket.setReceiveBufferSize(8 * 1024 * 1024);
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
            Thread.ofVirtual().name("rx-br-tick-" + playerId).start(() -> {
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
                                byte tag = buf[payloadStart]; // 0x20: BR_TICK
                                if (tag == TAG_BR_TICK) {
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
                                    if ((ticksReceived.get() & 255) == 0) {
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
         * Runs the high-precision 30 Hz Battle Royale Movement & Position Loop (~33.3 ms per tick).
         */
        void startGameLoop() {
            Thread.ofVirtual().name("tx-br-tick-" + playerId).start(() -> {
                long nextTickTime = System.nanoTime();

                while (running.get() && !udpSocket.isClosed()) {
                    try {
                        long tickStart = System.currentTimeMillis();
                        sequenceNumber++;

                        // Simulate movement across 8km map (walking, running, or driving UAZ vehicle)
                        posX += ThreadLocalRandom.current().nextFloat(-2.0f, 2.0f);
                        posZ += ThreadLocalRandom.current().nextFloat(-2.0f, 2.0f);
                        heading += ThreadLocalRandom.current().nextFloat(-1.5f, 1.5f);

                        // Structure realistic 72-byte Battle Royale Movement & State Packet
                        ByteBuffer buf = ByteBuffer.allocate(72);
                        buf.put(TAG_BR_TICK);                 // 1 byte: Stream Tag (0x20)
                        buf.putLong(tickStart);               // 8 bytes: Timestamp
                        buf.putInt(sequenceNumber);           // 4 bytes: Sequence #
                        buf.putFloat(posX);                   // 4 bytes: Map Pos X (0-8000m)
                        buf.putFloat(posY);                   // 4 bytes: Altitude Pos Y
                        buf.putFloat(posZ);                   // 4 bytes: Map Pos Z (0-8000m)
                        buf.putFloat(heading);                // 4 bytes: Direction Compass Heading
                        buf.putShort((short) 0b000000000010); // 2 bytes: Stance (Sprinting / Driving)
                        buf.put((byte) 100);                  // 1 byte: Health
                        buf.put((byte) 75);                   // 1 byte: Energy / Boost
                        buf.putShort((short) 4);              // 2 bytes: Weapon ID (M416)
                        buf.putShort((short) 40);             // 2 bytes: Ammo count
                        buf.putFloat(15.2f);                  // 4 bytes: Vehicle / Movement Speed (m/s)
                        buf.put(new byte[31]);                // 31 bytes: Inventory bitmask & delta payload = 72B total

                        Packet tickPacket = Packet.ofBroadcast(unpreservedTickSession, buf.array());
                        sendUdpPacket(tickPacket);
                        ticksSent.incrementAndGet();

                        // High precision 30 Hz pacing
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

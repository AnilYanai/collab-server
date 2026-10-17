package com.anilyanai.multisessionbenchmark;

import com.anilyanai.business.security.HmacTokenAuthorizer;
import com.anilyanai.protocol.Opcode;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.BinaryPacketCodec;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * =========================================================================================
 * 🚀 MultiSessionUDPBenchmark (Exact TCP Benchmark Match: 5 Rooms × 20 Clients, 200 msgs)
 * =========================================================================================
 *
 * <p>Identical workload to {@link MultiSessionBenchmark} (TCP) running over UDP datagrams:</p>
 * <ul>
 *   <li>5 distinct chat sessions / rooms</li>
 *   <li>20 clients per room (100 clients total)</li>
 *   <li>200 messages per client (20,000 total inbound datagrams)</li>
 *   <li>380,000 expected broadcasts (19 peers fan-out per room)</li>
 *   <li>0 ms send interval (maximum burst speed)</li>
 *   <li>Cryptographic HMAC-SHA256 session tokens with valid room scopes</li>
 * </ul>
 */
public class MultiSessionUDPBenchmark {

    // =========================================================================
    // CONFIGURATION (IDENTICAL TO TCP BENCHMARK)
    // =========================================================================
    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    // Cryptographic HMAC Secret (matches application.properties)
    private static final String SHARED_HMAC_SECRET = "collab-benchmark-secret-key-256bit-ok";

    private static final int NUM_SESSIONS = 5;                // 5 distinct chat sessions/rooms
    private static final int CLIENTS_PER_SESSION = 20;         // 20 clients per room
    private static final int TOTAL_CLIENTS = NUM_SESSIONS * CLIENTS_PER_SESSION; // 100 clients total

    private static final int MESSAGES_PER_CLIENT = 200;       // 200 messages per client
    private static final int SEND_INTERVAL_MS = 0;            // 0 ms = Maximum burst speed
    private static final int WARMUP_DELAY_MS = 1000;          // Time for all ACKs to settle
    private static final int IDLE_SETTLE_TIMEOUT_SEC = 3;     // Settle wait timeout
    private static final int SOCKET_BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB socket buffer

    // =========================================================================
    // METRICS
    // =========================================================================
    private static final AtomicInteger totalMessagesSent = new AtomicInteger(0);
    private static final AtomicLong totalMessagesReceived = new AtomicLong(0);
    private static final AtomicLong totalBytesSent = new AtomicLong(0);
    private static final AtomicLong totalBytesReceived = new AtomicLong(0);
    private static final AtomicInteger totalErrors = new AtomicInteger(0);
    private static final AtomicInteger sessionIsolationViolations = new AtomicInteger(0);
    private static final ConcurrentLinkedQueue<Long> latenciesMs = new ConcurrentLinkedQueue<>();

    private static final AtomicBoolean isBenchmarkActive = new AtomicBoolean(false);
    private static final CountDownLatch allDeliveriesLatch = new CountDownLatch(1);

    private static final BinaryPacketCodec CODEC = new BinaryPacketCodec();

    public static void main(String[] args) throws Exception {
        String runId = UUID.randomUUID().toString().substring(0, 8);

        int expectedTotalSent = TOTAL_CLIENTS * MESSAGES_PER_CLIENT;
        int fanOutPerRoom = CLIENTS_PER_SESSION - 1;
        long expectedTotalReceived = (long) expectedTotalSent * fanOutPerRoom;

        System.out.println("=================================================================");
        System.out.println("🔬 MULTI-SESSION UDP BENCHMARK (TCP-EQUIVALENT: 5 ROOMS × 20 CLIENTS)");
        System.out.println("=================================================================");
        System.out.printf("Benchmark Run ID         : %s%n", runId);
        System.out.printf("Target Server            : %s:%d (UDP)%n", HOST, PORT);
        System.out.printf("Total Sessions / Rooms   : %d%n", NUM_SESSIONS);
        System.out.printf("Clients per Session      : %d (Total Clients: %d)%n", CLIENTS_PER_SESSION, TOTAL_CLIENTS);
        System.out.printf("Messages per Client      : %d (Total Inbound Sent: %,d)%n", MESSAGES_PER_CLIENT, expectedTotalSent);
        System.out.printf("Expected Broadcasts Recv : %,d (Fan-Out / Room: %d)%n", expectedTotalReceived, fanOutPerRoom);
        System.out.printf("Send Interval            : %d ms (Full Burst)%n", SEND_INTERVAL_MS);
        System.out.printf("Security Mode            : Signed HMAC-SHA256 Room Tokens%n");
        System.out.println("=================================================================\n");

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch joinAckLatch = new CountDownLatch(TOTAL_CLIENTS);
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch allSendCompleteLatch = new CountDownLatch(TOTAL_CLIENTS);
        InetSocketAddress serverAddress = new InetSocketAddress(HOST, PORT);

        List<SimulatedUDPSessionClient> clients = new ArrayList<>(TOTAL_CLIENTS);

        System.out.printf("⏳ Spawning %,d UDP clients across %,d dynamically-generated rooms...%n", TOTAL_CLIENTS, NUM_SESSIONS);
        for (int s = 0; s < NUM_SESSIONS; s++) {
            String sessionId = "bench-udp-" + runId + "-room-" + s;
            System.out.printf("  • Registered UDP Room [%d]: %s%n", s, sessionId);
            for (int c = 1; c <= CLIENTS_PER_SESSION; c++) {
                int clientId = (s * CLIENTS_PER_SESSION) + c;
                String userId = "user-" + clientId;

                String roomToken = HmacTokenAuthorizer.generateToken(
                        sessionId,
                        userId,
                        600,
                        SHARED_HMAC_SECRET
                );

                SimulatedUDPSessionClient client = new SimulatedUDPSessionClient(
                        clientId, sessionId, roomToken, serverAddress, joinAckLatch, startSignal, allSendCompleteLatch, expectedTotalReceived
                );
                clients.add(client);
                executor.submit(client);
            }
        }

        if (!joinAckLatch.await(30, TimeUnit.SECONDS)) {
            long remaining = joinAckLatch.getCount();
            System.err.printf("⚠️ Warning: %d / %d UDP clients did not receive join ACKs in time.%n", remaining, TOTAL_CLIENTS);
        } else {
            System.out.println("✅ All " + TOTAL_CLIENTS + " UDP clients joined rooms and received server ACKs!");
        }

        System.out.printf("⏳ Warming up (%d ms) for server UDP routing tables to settle...%n", WARMUP_DELAY_MS);
        Thread.sleep(WARMUP_DELAY_MS);

        totalMessagesSent.set(0);
        totalMessagesReceived.set(0);
        totalBytesSent.set(0);
        totalBytesReceived.set(0);

        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        long benchmarkStartTime = System.currentTimeMillis();
        isBenchmarkActive.set(true);

        reporter.scheduleAtFixedRate(() -> {
            if (!isBenchmarkActive.get()) return;
            long elapsedSec = Math.max(1, (System.currentTimeMillis() - benchmarkStartTime) / 1000);
            int sent = totalMessagesSent.get();
            long recv = totalMessagesReceived.get();
            double progress = expectedTotalReceived > 0 ? ((double) recv / expectedTotalReceived) * 100.0 : 0.0;

            System.out.printf("[⏱️ %2ds] Sent: %,5d | Recv: %,7d / %,7d (%.1f%%) | Leaks: %d | Err: %d%n",
                    elapsedSec, sent, recv, expectedTotalReceived, progress,
                    sessionIsolationViolations.get(), totalErrors.get());
        }, 1, 1, TimeUnit.SECONDS);

        System.out.println("\n-----------------------------------------------------------------");
        System.out.println("🔥 FIRING UDP BROADCAST BURST ACROSS ALL SESSIONS...");
        System.out.println("-----------------------------------------------------------------\n");

        startSignal.countDown();

        allSendCompleteLatch.await();
        long sendEndTime = System.currentTimeMillis();
        double sendDurationSec = (sendEndTime - benchmarkStartTime) / 1000.0;

        System.out.printf("📤 [Phase 1: Inbound] All UDP clients finished bursting in %.2f seconds (%,.2f msg/sec).%n",
                sendDurationSec, expectedTotalSent / Math.max(0.001, sendDurationSec));
        System.out.printf("⏳ [Phase 2: Outbound] Waiting for multi-room UDP broadcasts to complete...%n");

        long lastRecvCount = 0;
        int idleSeconds = 0;
        long broadcastEndTime = System.currentTimeMillis();

        while (isBenchmarkActive.get()) {
            boolean allDone = allDeliveriesLatch.await(1, TimeUnit.SECONDS);
            long currentRecv = totalMessagesReceived.get();

            if (allDone || currentRecv >= expectedTotalReceived) {
                broadcastEndTime = System.currentTimeMillis();
                System.out.printf("🎉 All %,d UDP broadcast messages received!%n", currentRecv);
                break;
            }

            if (currentRecv == lastRecvCount && currentRecv > 0) {
                idleSeconds++;
                if (idleSeconds >= IDLE_SETTLE_TIMEOUT_SEC) {
                    broadcastEndTime = System.currentTimeMillis() - (IDLE_SETTLE_TIMEOUT_SEC * 1000L);
                    System.out.println("\n✅ UDP traffic settled. Finalizing report...");
                    break;
                }
            } else {
                idleSeconds = 0;
                lastRecvCount = currentRecv;
                broadcastEndTime = System.currentTimeMillis();
            }
        }

        isBenchmarkActive.set(false);
        reporter.shutdown();

        double broadcastDurationSec = Math.max(0.001, (broadcastEndTime - benchmarkStartTime) / 1000.0);

        for (SimulatedUDPSessionClient client : clients) {
            client.close();
        }
        executor.shutdownNow();

        printSummary(runId, expectedTotalSent, expectedTotalReceived, sendDurationSec, broadcastDurationSec);
    }

    private static void printSummary(String runId, int expectedSent, long expectedReceived, double sendSec, double broadcastSec) {
        int sent = totalMessagesSent.get();
        long recv = totalMessagesReceived.get();
        double deliveryRatio = expectedReceived > 0 ? ((double) recv / expectedReceived) * 100.0 : 0.0;
        long lostPackets = Math.max(0, expectedReceived - recv);

        System.out.println("\n=================================================================");
        System.out.printf("🏁 MULTI-SESSION UDP BENCHMARK REPORT (RUN: %s)%n", runId);
        System.out.println("=================================================================");
        System.out.printf("Total Sessions                   : %d%n", NUM_SESSIONS);
        System.out.printf("Total Clients                    : %d (%d / session)%n", TOTAL_CLIENTS, CLIENTS_PER_SESSION);
        System.out.printf("Total Messages Sent (Inbound)    : %,d (Expected: %,d)%n", sent, expectedSent);
        System.out.printf("Total Broadcasts Recv (Outbound) : %,d (Expected: %,d)%n", recv, expectedReceived);
        System.out.printf("Packets Dropped / Lost (UDP)     : %,d%n", lostPackets);
        System.out.printf("Delivery Success Rate            : %.2f%%%n", deliveryRatio);
        System.out.printf("Session Isolation Violations     : %d (0 = perfect room isolation)%n", sessionIsolationViolations.get());
        System.out.printf("Total Errors Encountered         : %d%n", totalErrors.get());
        System.out.println("-----------------------------------------------------------------");
        System.out.println("⏱️  THROUGHPUT SUMMARY:");
        System.out.printf("  • Client -> Server Inbound Rate : %,.2f msg/sec (%.2f s)%n", sent / Math.max(0.001, sendSec), sendSec);
        System.out.printf("  • Server -> Client Fan-out Rate : %,.2f msg/sec (%.2f s)%n", recv / Math.max(0.001, broadcastSec), broadcastSec);
        System.out.println("-----------------------------------------------------------------");

        System.out.println("\n📈 LATENCY DISTRIBUTION (ms):");
        if (latenciesMs.isEmpty()) {
            System.out.println("  No latency data recorded.");
        } else {
            List<Long> sorted = new ArrayList<>(latenciesMs);
            Collections.sort(sorted);
            int size = sorted.size();
            long min = sorted.getFirst();
            long max = sorted.getLast();
            double avg = sorted.stream().mapToLong(Long::longValue).average().orElse(0.0);
            long p50 = sorted.get((int) (size * 0.50));
            long p90 = sorted.get((int) (size * 0.90));
            long p95 = sorted.get((int) (size * 0.95));
            long p99 = sorted.get((int) (size * 0.99));

            System.out.printf("  Samples: %,d | Min: %d ms | Avg: %.2f ms | Max: %d ms%n", size, min, avg, max);
            System.out.printf("  Percentiles: P50: %d ms | P90: %d ms | P95: %d ms | P99: %d ms%n", p50, p90, p95, p99);
        }
        System.out.println("=================================================================\n");
    }

    /**
     * Simulated UDP client connecting to a specific room using protocol datagrams.
     */
    private static class SimulatedUDPSessionClient implements Runnable {
        private final int clientId;
        private final String sessionId;
        private final String roomToken;
        private final InetSocketAddress serverAddress;
        private final CountDownLatch joinAckLatch;
        private final CountDownLatch startSignal;
        private final CountDownLatch allSendCompleteLatch;
        private final long expectedTotalRecv;

        private DatagramSocket socket;
        private volatile boolean running = true;
        private final AtomicBoolean joined = new AtomicBoolean(false);

        public SimulatedUDPSessionClient(int clientId,
                                         String sessionId,
                                         String roomToken,
                                         InetSocketAddress serverAddress,
                                         CountDownLatch joinAckLatch,
                                         CountDownLatch startSignal,
                                         CountDownLatch allSendCompleteLatch,
                                         long expectedTotalRecv) {
            this.clientId = clientId;
            this.sessionId = sessionId;
            this.roomToken = roomToken;
            this.serverAddress = serverAddress;
            this.joinAckLatch = joinAckLatch;
            this.startSignal = startSignal;
            this.allSendCompleteLatch = allSendCompleteLatch;
            this.expectedTotalRecv = expectedTotalRecv;
        }

        @Override
        public void run() {
            try {
                socket = new DatagramSocket();
                socket.setReceiveBufferSize(SOCKET_BUFFER_SIZE);
                socket.setSendBufferSize(SOCKET_BUFFER_SIZE);

                Thread.ofVirtual().name("udp-client-reader-" + clientId).start(this::readLoop);

                // 1. Send Protocol JOIN_SESSION packet via UDP Datagram
                byte[] tokenBytes = roomToken.getBytes(StandardCharsets.UTF_8);
                Packet joinPacket = new Packet(
                        Packet.CURRENT_VERSION,
                        Opcode.JOIN_SESSION,
                        Packet.FLAG_NONE,
                        clientId,
                        (short) 0,
                        (short) 1,
                        sessionId,
                        null,
                        tokenBytes
                );

                byte[] joinBytes = CODEC.encode(joinPacket);
                DatagramPacket joinDatagram = new DatagramPacket(joinBytes, joinBytes.length, serverAddress);
                socket.send(joinDatagram);

                // Wait for synchronized benchmark start
                startSignal.await();

                // 2. Burst BROADCAST datagrams to room (0 ms interval = maximum burst speed)
                for (int seq = 1; seq <= MESSAGES_PER_CLIENT; seq++) {
                    long now = System.currentTimeMillis();
                    String chatText = "[CHAT]|c" + clientId + "|" + sessionId + "|" + seq + "|" + now;
                    byte[] payload = chatText.getBytes(StandardCharsets.UTF_8);

                    Packet broadcastPacket = Packet.ofBroadcast(sessionId, payload);
                    byte[] packetBytes = CODEC.encode(broadcastPacket);

                    DatagramPacket packet = new DatagramPacket(packetBytes, packetBytes.length, serverAddress);
                    socket.send(packet);

                    totalMessagesSent.incrementAndGet();
                    totalBytesSent.addAndGet(packetBytes.length);

                    if (SEND_INTERVAL_MS > 0) {
                        Thread.sleep(SEND_INTERVAL_MS);
                    }
                }

            } catch (Exception e) {
                totalErrors.incrementAndGet();
            } finally {
                allSendCompleteLatch.countDown();
            }
        }

        private void readLoop() {
            byte[] buffer = new byte[8192];
            DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);

            while (running) {
                try {
                    socket.receive(datagram);
                    long recvTime = System.currentTimeMillis();

                    byte[] packetData = Arrays.copyOf(datagram.getData(), datagram.getLength());

                    try {
                        Packet packet = CODEC.decode(packetData);

                        if (packet.opcode() == Opcode.ACK) {
                            if (joined.compareAndSet(false, true)) {
                                joinAckLatch.countDown();
                            }
                            continue;
                        }

                        if (!sessionId.equals(packet.sessionId())) {
                            sessionIsolationViolations.incrementAndGet();
                        }

                        long count = totalMessagesReceived.incrementAndGet();
                        totalBytesReceived.addAndGet(datagram.getLength());

                        parseTimestamp(packet.payload(), recvTime);

                        if (count >= expectedTotalRecv) {
                            allDeliveriesLatch.countDown();
                        }
                    } catch (Exception decodeEx) {
                        totalErrors.incrementAndGet();
                    }
                } catch (SocketException e) {
                    break;
                } catch (IOException e) {
                    if (running) {
                        totalErrors.incrementAndGet();
                    }
                } finally {
                    if (joined.compareAndSet(false, true)) {
                        joinAckLatch.countDown();
                    }
                }
            }
        }

        private void parseTimestamp(byte[] payload, long recvTime) {
            if (payload == null || payload.length == 0) return;
            String text = new String(payload, StandardCharsets.UTF_8);
            String[] parts = text.split("\\|");
            if (parts.length >= 5) {
                try {
                    long sentTime = Long.parseLong(parts[4]);
                    latenciesMs.add(Math.max(0, recvTime - sentTime));
                } catch (NumberFormatException ignored) {
                }
            }
        }

        public void close() {
            running = false;
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }
}

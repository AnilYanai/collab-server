package com.anilyanai.multisessionbenchmark;

import com.anilyanai.business.security.HmacTokenAuthorizer;
import com.anilyanai.protocol.Opcode;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.BinaryPacketCodec;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * =========================================================================================
 * 🚀 MultiSessionBenchmark (50 Sessions × 50 Clients = 2,500 TCP Sockets with HMAC Tokens)
 * =========================================================================================
 *
 * <p>Stress tests TCP connection handling, room multiplexing, and outbound gathering writes:</p>
 * <ul>
 *   <li>50 distinct chat sessions / rooms</li>
 *   <li>50 clients per room (2,500 concurrent TCP persistent connections)</li>
 *   <li>20 messages per client (50,000 total inbound protocol packets)</li>
 *   <li>2,450,000 expected broadcasts (49 peers fan-out per room)</li>
 *   <li>0 ms send interval (maximum burst speed)</li>
 *   <li>Full HMAC-SHA256 token authorization per client</li>
 * </ul>
 */
public class MultiSessionBenchmark {

    // =========================================================================
    // CONFIGURATION (50 SESSIONS × 50 CLIENTS = 2,500 CLIENTS)
    // =========================================================================
    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    // Cryptographic HMAC Secret (shared with server)
    private static final String SHARED_HMAC_SECRET = "collab-benchmark-secret-key-256bit-ok";

    private static final int NUM_SESSIONS = 50;               // 50 distinct chat sessions/rooms
    private static final int CLIENTS_PER_SESSION = 50;        // 50 clients per room
    private static final int TOTAL_CLIENTS = NUM_SESSIONS * CLIENTS_PER_SESSION; // 2,500 clients total

    private static final int MESSAGES_PER_CLIENT = 20;        // 20 messages per client
    private static final int SEND_INTERVAL_MS = 0;           // 0 ms = Maximum burst speed
    private static final int WARMUP_DELAY_MS = 1500;         // Time for all ACKs to settle
    private static final int IDLE_SETTLE_TIMEOUT_SEC = 5;    // Settle wait timeout
    private static final int CLIENT_SOCKET_BUFFER = 128 * 1024; // 128 KB per socket (safe for 2,500 sockets)

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
        System.out.println("🔬 MULTI-SESSION TCP BENCHMARK (50 ROOMS × 50 CLIENTS = 2,500 CLIENTS)");
        System.out.println("=================================================================");
        System.out.printf("Benchmark Run ID         : %s%n", runId);
        System.out.printf("Target Server            : %s:%d (TCP)%n", HOST, PORT);
        System.out.printf("Total Sessions / Rooms   : %d%n", NUM_SESSIONS);
        System.out.printf("Clients per Session      : %d (Total Clients: %,d)%n", CLIENTS_PER_SESSION, TOTAL_CLIENTS);
        System.out.printf("Messages per Client      : %d (Total Inbound Sent: %,d)%n", MESSAGES_PER_CLIENT, expectedTotalSent);
        System.out.printf("Expected Broadcasts Recv : %,d (Fan-Out / Room: %d)%n", expectedTotalReceived, fanOutPerRoom);
        System.out.printf("Send Interval            : %d ms (Full Burst)%n", SEND_INTERVAL_MS);
        System.out.printf("Security Mode            : Signed HMAC-SHA256 Room Tokens%n");
        System.out.println("=================================================================\\n");

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        CountDownLatch joinAckLatch = new CountDownLatch(TOTAL_CLIENTS);
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch allSendCompleteLatch = new CountDownLatch(TOTAL_CLIENTS);

        List<SimulatedSessionClient> clients = new ArrayList<>(TOTAL_CLIENTS);

        System.out.printf("⏳ Spawning & connecting %,d TCP clients across %,d dynamically-generated rooms...%n", TOTAL_CLIENTS, NUM_SESSIONS);
        for (int s = 0; s < NUM_SESSIONS; s++) {
            String sessionId = "bench-tcp-" + runId + "-room-" + s;
            if (s < 5 || s % 10 == 0) {
                System.out.printf("  • Registered Room [%d]: %s%n", s, sessionId);
            }
            for (int c = 1; c <= CLIENTS_PER_SESSION; c++) {
                int clientId = (s * CLIENTS_PER_SESSION) + c;
                String userId = "user-" + clientId;

                String roomToken = HmacTokenAuthorizer.generateToken(
                        sessionId,
                        userId,
                        600,
                        SHARED_HMAC_SECRET
                );

                SimulatedSessionClient client = new SimulatedSessionClient(
                        clientId, sessionId, roomToken, joinAckLatch, startSignal, allSendCompleteLatch, expectedTotalReceived
                );
                clients.add(client);
                executor.submit(client);
            }
        }

        if (!joinAckLatch.await(60, TimeUnit.SECONDS)) {
            long remaining = joinAckLatch.getCount();
            System.err.printf("⚠️ Warning: %d / %d clients did not finish connecting and receiving join ACKs in time.%n", remaining, TOTAL_CLIENTS);
        } else {
            System.out.println("✅ All " + TOTAL_CLIENTS + " TCP clients successfully connected, joined rooms with cryptographic tokens, and received server ACKs!");
        }

        System.out.printf("⏳ Warming up (%d ms) for server routing tables to settle...%n", WARMUP_DELAY_MS);
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

            System.out.printf("[⏱️ %2ds] Sent: %,6d | Recv: %,8d / %,8d (%.1f%%) | Leaks: %d | Err: %d%n",
                    elapsedSec, sent, recv, expectedTotalReceived, progress,
                    sessionIsolationViolations.get(), totalErrors.get());
        }, 1, 1, TimeUnit.SECONDS);

        System.out.println("\n-----------------------------------------------------------------");
        System.out.println("🔥 FIRING TCP PROTOCOL BROADCAST BURST ACROSS ALL SESSIONS...");
        System.out.println("-----------------------------------------------------------------\n");

        startSignal.countDown();

        allSendCompleteLatch.await();
        long sendEndTime = System.currentTimeMillis();
        double sendDurationSec = (sendEndTime - benchmarkStartTime) / 1000.0;

        System.out.printf("📤 [Phase 1: Inbound] All TCP clients finished bursting in %.2f seconds (%,.2f msg/sec).%n",
                sendDurationSec, expectedTotalSent / Math.max(0.001, sendDurationSec));
        System.out.printf("⏳ [Phase 2: Outbound] Waiting for multi-room TCP broadcasts to complete...%n");

        long lastRecvCount = 0;
        int idleSeconds = 0;
        long broadcastEndTime = System.currentTimeMillis();

        while (isBenchmarkActive.get()) {
            boolean allDone = allDeliveriesLatch.await(1, TimeUnit.SECONDS);
            long currentRecv = totalMessagesReceived.get();

            if (allDone || currentRecv >= expectedTotalReceived) {
                broadcastEndTime = System.currentTimeMillis();
                System.out.printf("🎉 All %,d TCP broadcast messages received!%n", currentRecv);
                break;
            }

            if (currentRecv == lastRecvCount && currentRecv > 0) {
                idleSeconds++;
                if (idleSeconds >= IDLE_SETTLE_TIMEOUT_SEC) {
                    broadcastEndTime = System.currentTimeMillis() - (IDLE_SETTLE_TIMEOUT_SEC * 1000L);
                    System.out.println("\n✅ TCP traffic settled. Finalizing report...");
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

        for (SimulatedSessionClient client : clients) {
            client.close();
        }
        executor.shutdownNow();

        printSummary(runId, expectedTotalSent, expectedTotalReceived, sendDurationSec, broadcastDurationSec);
    }

    private static void printSummary(String runId, int expectedSent, long expectedReceived, double sendSec, double broadcastSec) {
        int sent = totalMessagesSent.get();
        long recv = totalMessagesReceived.get();
        double deliveryRatio = expectedReceived > 0 ? ((double) recv / expectedReceived) * 100.0 : 0.0;

        System.out.println("\n=================================================================");
        System.out.printf("🏁 MULTI-SESSION TCP BENCHMARK REPORT (RUN: %s)%n", runId);
        System.out.println("=================================================================");
        System.out.printf("Total Sessions                   : %d%n", NUM_SESSIONS);
        System.out.printf("Total Clients                    : %,d (%d / session)%n", TOTAL_CLIENTS, CLIENTS_PER_SESSION);
        System.out.printf("Total Messages Sent (Inbound)    : %,d (Expected: %,d)%n", sent, expectedSent);
        System.out.printf("Total Broadcasts Recv (Outbound) : %,d (Expected: %,d)%n", recv, expectedReceived);
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
        System.out.println("=================================================================\\n");
    }

    /**
     * Simulated Client joining a specific room using protocol framing and cryptographic token.
     */
    private static class SimulatedSessionClient implements Runnable {
        private final int clientId;
        private final String sessionId;
        private final byte[] sessionIdBytes;
        private final String roomToken;
        private final CountDownLatch joinAckLatch;
        private final CountDownLatch startSignal;
        private final CountDownLatch allSendCompleteLatch;
        private final long expectedTotalRecv;

        private Socket socket;
        private InputStream in;
        private OutputStream out;
        private volatile boolean running = true;
        private final AtomicBoolean joined = new AtomicBoolean(false);

        public SimulatedSessionClient(int clientId,
                                      String sessionId,
                                      String roomToken,
                                      CountDownLatch joinAckLatch,
                                      CountDownLatch startSignal,
                                      CountDownLatch allSendCompleteLatch,
                                      long expectedTotalRecv) {
            this.clientId = clientId;
            this.sessionId = sessionId;
            this.sessionIdBytes = sessionId.getBytes(StandardCharsets.UTF_8);
            this.roomToken = roomToken;
            this.joinAckLatch = joinAckLatch;
            this.startSignal = startSignal;
            this.allSendCompleteLatch = allSendCompleteLatch;
            this.expectedTotalRecv = expectedTotalRecv;
        }

        @Override
        public void run() {
            try {
                socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setReceiveBufferSize(CLIENT_SOCKET_BUFFER);
                socket.setSendBufferSize(CLIENT_SOCKET_BUFFER);
                socket.connect(new InetSocketAddress(HOST, PORT), 10000);

                in = socket.getInputStream();
                out = new BufferedOutputStream(socket.getOutputStream(), 32 * 1024);

                Thread.ofVirtual().name("tcp-client-reader-" + clientId).start(this::readLoop);

                // 1. Send Protocol JOIN_SESSION packet with Cryptographic Token Payload
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
                writeFrame(joinBytes);
                out.flush();

                // Wait for synchronized benchmark start
                startSignal.await();

                // 2. Burst BROADCAST messages to room
                for (int seq = 1; seq <= MESSAGES_PER_CLIENT; seq++) {
                    long now = System.currentTimeMillis();
                    String chatText = "[CHAT]|c" + clientId + "|" + sessionId + "|" + seq + "|" + now;
                    byte[] payload = chatText.getBytes(StandardCharsets.UTF_8);

                    Packet broadcastPacket = Packet.ofBroadcast(sessionId, payload);
                    byte[] packetBytes = CODEC.encode(broadcastPacket);

                    writeFrame(packetBytes);

                    totalMessagesSent.incrementAndGet();
                    totalBytesSent.addAndGet(4 + packetBytes.length);

                    if (SEND_INTERVAL_MS > 0) {
                        Thread.sleep(SEND_INTERVAL_MS);
                    }
                }
                out.flush();

            } catch (Exception e) {
                totalErrors.incrementAndGet();
            } finally {
                allSendCompleteLatch.countDown();
            }
        }

        private void writeFrame(byte[] bytes) throws IOException {
            out.write((bytes.length >> 24) & 0xFF);
            out.write((bytes.length >> 16) & 0xFF);
            out.write((bytes.length >> 8) & 0xFF);
            out.write(bytes.length & 0xFF);
            out.write(bytes);
        }

        private void readLoop() {
            try {
                byte[] header = new byte[4];
                byte[] buffer = new byte[8192];

                while (running) {
                    readFully(in, header, 0, 4);
                    int frameLen = ((header[0] & 0xFF) << 24)
                            | ((header[1] & 0xFF) << 16)
                            | ((header[2] & 0xFF) << 8)
                            | (header[3] & 0xFF);

                    if (frameLen < 0 || frameLen > 16 * 1024 * 1024) break;

                    byte[] packetData = (frameLen <= buffer.length) ? buffer : new byte[frameLen];
                    readFully(in, packetData, 0, frameLen);

                    long recvTime = System.currentTimeMillis();

                    // Wire protocol layout:
                    // [0]=MAGIC(0xCA), [1]=Version, [2]=Opcode, [3]=Flags, [4..7]=MsgId, [8..9]=Chunk, [10..11]=TotalChunks
                    // [12]=SessionIdLen, [13..13+SessionIdLen-1]=SessionId
                    byte opcode = packetData[2];

                    if (opcode == Opcode.ACK.getCode()) {
                        if (joined.compareAndSet(false, true)) {
                            joinAckLatch.countDown();
                        }
                        continue;
                    }

                    // ⚡ Zero-allocation fast-path for broadcast packets:
                    // Verify session isolation directly via SIMD byte comparison (0 heap objects!)
                    int sessionLen = Byte.toUnsignedInt(packetData[12]);
                    if (sessionLen != sessionIdBytes.length || !Arrays.equals(packetData, 13, 13 + sessionLen, sessionIdBytes, 0, sessionLen)) {
                        sessionIsolationViolations.incrementAndGet();
                    }

                    long count = totalMessagesReceived.incrementAndGet();
                    totalBytesReceived.addAndGet(4 + frameLen);

                    // Sample 1 in 32 for latency to save memory on 2.45M packets
                    if ((count & 0x1F) == 0 && latenciesMs.size() < 100_000) {
                        parseTimestampFast(packetData, sessionLen, recvTime);
                    }

                    if (count >= expectedTotalRecv) {
                        allDeliveriesLatch.countDown();
                    }
                }
            } catch (EOFException | SocketException ignored) {
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

        private static void readFully(InputStream in, byte[] b, int off, int len) throws IOException {
            while (len > 0) {
                int n = in.read(b, off, len);
                if (n < 0) throw new EOFException();
                off += n;
                len -= n;
            }
        }

        private void parseTimestampFast(byte[] data, int sessionLen, long recvTime) {
            try {
                byte flags = data[3];
                boolean isDirect = (flags & Packet.FLAG_DIRECT) != 0;
                int offset = 13 + sessionLen;
                if (isDirect) {
                    offset += 1 + Byte.toUnsignedInt(data[offset]);
                }
                int payloadLen = ((data[offset] & 0xFF) << 24)
                        | ((data[offset + 1] & 0xFF) << 16)
                        | ((data[offset + 2] & 0xFF) << 8)
                        | (data[offset + 3] & 0xFF);
                if (payloadLen <= 0) return;
                int payloadStart = offset + 4;

                String text = new String(data, payloadStart, payloadLen, StandardCharsets.UTF_8);
                String[] parts = text.split("\\|");
                if (parts.length >= 5) {
                    long sentTime = Long.parseLong(parts[4]);
                    latenciesMs.add(Math.max(0, recvTime - sentTime));
                }
            } catch (Exception ignored) {
            }
        }

        public void close() {
            running = false;
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException ignored) {
            }
        }
    }
}

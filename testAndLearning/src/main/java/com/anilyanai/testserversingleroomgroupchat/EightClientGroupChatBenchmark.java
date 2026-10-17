package com.anilyanai.testserversingleroomgroupchat;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * =========================================================================================
 * 🔬 HIGH-THROUGHPUT MULTI-CLIENT (8 CLIENTS x 50,000 MSGS) MEGA BURST BENCHMARK
 * =========================================================================================
 *
 * Workload:
 * - 8 Concurrent Connected Clients.
 * - Each client sends 50,000 messages (Total Inbound Sent = 400,000 messages).
 * - Fan-Out Multiplier = 7 (Total Expected Outbound Broadcasts = 2,800,000 messages).
 * - Total Combined Network I/O = 3.2 Million Socket Operations!
 * - High-speed 128KB Inbound / 64KB Outbound Client Buffering + 4MB Socket Kernel Windows.
 */
public class EightClientGroupChatBenchmark {

    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    // Load settings:
    private static final int NUM_CLIENTS = 8;              // 8 Concurrent Clients
    private static final int MESSAGES_PER_CLIENT = 50_000; // 50,000 msgs each = 400,000 total messages sent
    private static final int SEND_INTERVAL_MS = 0;         // 0 ms = Maximum burst speed
    private static final int WARMUP_DELAY_MS = 2000;       // Settling time after registration
    private static final int IDLE_SETTLE_TIMEOUT_SEC = 3;  // Auto-finish if no new messages arrive for 3 seconds

    // Protocol Mode: "TCP_ONLY" (8 TCP), "UDP_ONLY" (8 UDP), or "HYBRID" (4 TCP + 4 UDP)
    private static final ProtocolMode MODE = ProtocolMode.TCP_ONLY;

    public enum ProtocolMode {
        TCP_ONLY,
        UDP_ONLY,
        HYBRID
    }

    // Metrics
    private static final AtomicInteger totalMessagesSent = new AtomicInteger(0);
    private static final AtomicInteger totalMessagesReceived = new AtomicInteger(0);
    private static final AtomicInteger totalErrors = new AtomicInteger(0);
    private static final AtomicLong totalBytesSent = new AtomicLong(0);
    private static final AtomicLong totalBytesReceived = new AtomicLong(0);

    private static final ConcurrentLinkedQueue<Long> latenciesMs = new ConcurrentLinkedQueue<>();
    private static final AtomicBoolean isTestActive = new AtomicBoolean(false);
    private static final CountDownLatch allDeliveriesLatch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        int expectedTotalSent = NUM_CLIENTS * MESSAGES_PER_CLIENT;
        int fanOutMultiplier = NUM_CLIENTS - 1;
        long expectedTotalReceived = (long) expectedTotalSent * fanOutMultiplier;

        System.out.println("=================================================================");
        System.out.println("🔬 MEGA MULTI-CLIENT (8 CLIENTS x 50,000 MSGS) BENCHMARK");
        System.out.println("=================================================================");
        System.out.printf("Target Server            : %s:%d%n", HOST, PORT);
        System.out.printf("Benchmark Mode           : %s%n", MODE);
        System.out.printf("Total Clients            : %d concurrent clients%n", NUM_CLIENTS);
        System.out.printf("Messages per Client      : %,d (Total Inbound Sent: %,d)%n", MESSAGES_PER_CLIENT, expectedTotalSent);
        System.out.printf("Expected Broadcasts Recv : %,d (Fan-Out Multiplier: %d)%n", expectedTotalReceived, fanOutMultiplier);
        System.out.printf("Send Interval / Client   : %d ms (BURST SPEED with 128KB In / 64KB Out Buffering)%n", SEND_INTERVAL_MS);
        System.out.println("=================================================================\n");

        CountDownLatch connectLatch = new CountDownLatch(NUM_CLIENTS);
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch sendCompleteLatch = new CountDownLatch(NUM_CLIENTS);

        List<AutoCloseable> clients = new ArrayList<>();

        System.out.printf("⏳ Connecting/Registering %d clients and sending handshake...%n", NUM_CLIENTS);

        InetSocketAddress serverAddr = new InetSocketAddress(HOST, PORT);

        for (int i = 1; i <= NUM_CLIENTS; i++) {
            if (MODE == ProtocolMode.TCP_ONLY) {
                clients.add(new SimulatedTCPClient(i, connectLatch, startSignal, sendCompleteLatch, expectedTotalReceived));
            } else if (MODE == ProtocolMode.UDP_ONLY) {
                clients.add(new SimulatedUDPClient(i, serverAddr, connectLatch, startSignal, sendCompleteLatch, expectedTotalReceived));
            } else { // HYBRID: Half TCP, Half UDP
                if (i <= NUM_CLIENTS / 2) {
                    clients.add(new SimulatedTCPClient(i, connectLatch, startSignal, sendCompleteLatch, expectedTotalReceived));
                } else {
                    clients.add(new SimulatedUDPClient(i, serverAddr, connectLatch, startSignal, sendCompleteLatch, expectedTotalReceived));
                }
            }
        }

        // Start client worker threads
        for (AutoCloseable client : clients) {
            if (client instanceof Runnable runnable) {
                Thread.ofVirtual().start(runnable);
            }
        }

        if (!connectLatch.await(15, TimeUnit.SECONDS)) {
            System.err.println("❌ Error: Clients failed to connect in time.");
            return;
        }

        System.out.printf("✅ All %d clients connected and registered!%n", NUM_CLIENTS);
        System.out.printf("⏳ Warming up (%d ms) for server routing table to settle...%n", WARMUP_DELAY_MS);
        Thread.sleep(WARMUP_DELAY_MS);

        // Reset metrics after warmup handshake pings
        totalMessagesReceived.set(0);
        totalBytesReceived.set(0);

        // Start Live Reporter
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        long benchmarkStartTime = System.currentTimeMillis();
        isTestActive.set(true);

        reporter.scheduleAtFixedRate(() -> {
            if (!isTestActive.get()) return;
            long elapsedSec = Math.max(1, (System.currentTimeMillis() - benchmarkStartTime) / 1000);
            int sent = totalMessagesSent.get();
            int recv = totalMessagesReceived.get();
            double progress = ((double) recv / expectedTotalReceived) * 100.0;
            System.out.printf("[⏱️ %2ds] Sent: %,7d / %,7d | Recv Broadcasts: %,8d / %,8d (%.1f%%) | Err: %d%n",
                    elapsedSec, sent, expectedTotalSent, recv, expectedTotalReceived, progress, totalErrors.get());
        }, 1, 1, TimeUnit.SECONDS);

        // Fire traffic
        System.out.println("\n🔥 FIRING ALL 8 CLIENTS AT FULL BURST SPEED (50,000 MSGS EACH)...\n");
        startSignal.countDown();

        // 1. Wait for all clients to finish sending to server
        sendCompleteLatch.await();
        long sendEndTime = System.currentTimeMillis();
        double sendDurationSec = (sendEndTime - benchmarkStartTime) / 1000.0;

        System.out.printf("%n📤 [Phase 1: Inbound] All %d clients finished sending in %.2f seconds (%,.2f msg/sec).%n",
                NUM_CLIENTS, sendDurationSec, totalMessagesSent.get() / Math.max(0.001, sendDurationSec));
        System.out.printf("⏳ [Phase 2: Outbound] Waiting for server to finish broadcasting all %,d messages...%n", expectedTotalReceived);

        // 2. Dynamic wait with idle detection (finishes when all delivered OR when no new messages arrive for 3 seconds)
        long lastRecvCount = 0;
        int idleSeconds = 0;
        long broadcastEndTime = System.currentTimeMillis();

        while (isTestActive.get()) {
            boolean allDone = allDeliveriesLatch.await(1, TimeUnit.SECONDS);
            long currentRecv = totalMessagesReceived.get();

            if (allDone || currentRecv >= expectedTotalReceived) {
                broadcastEndTime = System.currentTimeMillis();
                System.out.printf("🎉 All %,d broadcast messages fully delivered and received!%n", currentRecv);
                break;
            }

            if (currentRecv == lastRecvCount && currentRecv > 0) {
                idleSeconds++;
                if (idleSeconds >= IDLE_SETTLE_TIMEOUT_SEC) {
                    broadcastEndTime = System.currentTimeMillis() - (IDLE_SETTLE_TIMEOUT_SEC * 1000L);
                    System.out.println("\n✅ Traffic complete (settled for 3 seconds). Finalizing benchmark report...");
                    break;
                }
            } else {
                idleSeconds = 0;
                lastRecvCount = currentRecv;
                broadcastEndTime = System.currentTimeMillis();
            }
        }

        isTestActive.set(false);
        reporter.shutdown();

        double broadcastDurationSec = Math.max(0.001, (broadcastEndTime - benchmarkStartTime) / 1000.0);

        // Cleanup
        for (AutoCloseable client : clients) {
            client.close();
        }

        // Generate Report
        printReport(expectedTotalSent, expectedTotalReceived, sendDurationSec, broadcastDurationSec);
    }

    private static void printReport(int expectedSent, long expectedReceived, double sendDurationSec, double broadcastDurationSec) {
        int sent = totalMessagesSent.get();
        int received = totalMessagesReceived.get();
        int errors = totalErrors.get();

        List<Long> latencyList = new ArrayList<>(latenciesMs);
        Collections.sort(latencyList);

        double clientSendThroughput = sent / Math.max(0.001, sendDurationSec);
        double serverBroadcastThroughput = received / Math.max(0.001, broadcastDurationSec);
        double deliveryRatio = expectedReceived > 0 ? ((double) received / expectedReceived) * 100.0 : 0.0;

        System.out.println("\n=================================================================");
        System.out.println("🏁 PERFORMANCE BENCHMARK REPORT (8 CLIENTS x 50,000 MSGS)");
        System.out.println("=================================================================");
        System.out.printf("Total Messages Sent (Inbound)    : %,d (Expected: %,d)%n", sent, expectedSent);
        System.out.printf("Total Broadcasts Recv (Outbound) : %,d (Expected: %,d)%n", received, expectedReceived);
        System.out.printf("Delivery Success Rate            : %.2f%%%n", deliveryRatio);
        System.out.printf("Total Data Transferred           : Inbound: %.2f MB | Outbound: %.2f MB%n",
                totalBytesSent.get() / (1024.0 * 1024.0), totalBytesReceived.get() / (1024.0 * 1024.0));
        System.out.printf("Total Errors Encountered         : %d%n", errors);
        System.out.println("-----------------------------------------------------------------");
        System.out.println("⏱️  PHASE TIMING & THROUGHPUT BREAKDOWN:");
        System.out.printf("  1. Client -> Server Inbound    : %.2f seconds (Throughput: %,.2f msg/sec)%n",
                sendDurationSec, clientSendThroughput);
        System.out.printf("  2. Server -> Client Broadcast  : %.2f seconds (Throughput: %,.2f msg/sec)%n",
                broadcastDurationSec, serverBroadcastThroughput);
        System.out.println("-----------------------------------------------------------------");

        if (!latencyList.isEmpty()) {
            long min = latencyList.get(0);
            long max = latencyList.get(latencyList.size() - 1);
            double avg = latencyList.stream().mapToLong(Long::longValue).average().orElse(0.0);
            long p50 = percentile(latencyList, 50);
            long p90 = percentile(latencyList, 90);
            long p95 = percentile(latencyList, 95);
            long p99 = percentile(latencyList, 99);

            System.out.println("📈 END-TO-END ROUND-TRIP LATENCY (ms):");
            System.out.printf("  Samples: %,d | Min: %d ms | Avg: %.2f ms | Max: %d ms%n", latencyList.size(), min, avg, max);
            System.out.printf("  Percentiles: P50: %d ms | P90: %d ms | P95: %d ms | P99: %d ms%n", p50, p90, p95, p99);
        } else {
            System.out.println("⚠️ No latency metrics recorded.");
        }
        System.out.println("=================================================================\n");
    }

    private static long percentile(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) return 0;
        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    // =========================================================================
    // TCP CLIENT (Uses 128KB In / 64KB Out Buffers + 4MB Socket Window)
    // =========================================================================
    private static class SimulatedTCPClient implements Runnable, AutoCloseable {
        private final int clientId;
        private final CountDownLatch connectLatch;
        private final CountDownLatch startSignal;
        private final CountDownLatch sendCompleteLatch;
        private final long expectedTotalRecv;

        private Socket socket;
        private DataInputStream in;
        private DataOutputStream out;
        private volatile boolean running = true;

        public SimulatedTCPClient(int clientId,
                                  CountDownLatch connectLatch,
                                  CountDownLatch startSignal,
                                  CountDownLatch sendCompleteLatch,
                                  long expectedTotalRecv) {
            this.clientId = clientId;
            this.connectLatch = connectLatch;
            this.startSignal = startSignal;
            this.sendCompleteLatch = sendCompleteLatch;
            this.expectedTotalRecv = expectedTotalRecv;
        }

        @Override
        public void run() {
            try {
                socket = new Socket(HOST, PORT);
                socket.setTcpNoDelay(true);
                // 4 MB OS Socket Kernel Buffers to prevent TCP Window drops
                socket.setReceiveBufferSize(4 * 1024 * 1024);
                socket.setSendBufferSize(4 * 1024 * 1024);

                // 128 KB Buffered Inbound Stream for high-speed batch read syscalls
                InputStream rawIn = socket.getInputStream();
                BufferedInputStream bufIn = new BufferedInputStream(rawIn, 128 * 1024);
                in = new DataInputStream(bufIn);

                // 64 KB Buffered Outbound Stream for line-rate burst writes
                OutputStream rawOut = socket.getOutputStream();
                BufferedOutputStream bufOut = new BufferedOutputStream(rawOut, 64 * 1024);
                out = new DataOutputStream(bufOut);

                Thread reader = new Thread(this::readLoop, "tcp-fast-reader-" + clientId);
                reader.setDaemon(true);
                reader.start();

                // Send discovery registration ping during warmup so server maps this connection ID
                byte[] warmupPing = ("[REGISTER]|C" + clientId).getBytes(StandardCharsets.UTF_8);
                out.writeInt(warmupPing.length);
                out.write(warmupPing);
                out.flush();

                connectLatch.countDown();
                startSignal.await();

                for (int seq = 1; seq <= MESSAGES_PER_CLIENT; seq++) {
                    long now = System.currentTimeMillis();
                    String msg = "[BENCHMARK]|C" + clientId + "|" + seq + "|" + now + "|msg_" + seq;
                    byte[] payload = msg.getBytes(StandardCharsets.UTF_8);

                    // Length-prefixed framing: 4-byte length header + payload
                    out.writeInt(payload.length);
                    out.write(payload);

                    totalMessagesSent.incrementAndGet();
                    totalBytesSent.addAndGet(payload.length);

                    if (SEND_INTERVAL_MS > 0) {
                        Thread.sleep(SEND_INTERVAL_MS);
                    }
                }
                out.flush();
            } catch (Exception e) {
                totalErrors.incrementAndGet();
            } finally {
                sendCompleteLatch.countDown();
            }
        }

        private void readLoop() {
            try {
                byte[] readBuffer = new byte[8192];
                while (running) {
                    int length = in.readInt();
                    if (length < 0 || length > 16 * 1024 * 1024) break;

                    byte[] payload = (length <= readBuffer.length) ? readBuffer : new byte[length];
                    in.readFully(payload, 0, length);

                    long recvTime = System.currentTimeMillis();
                    int count = totalMessagesReceived.incrementAndGet();
                    totalBytesReceived.addAndGet(length);

                    parseLatencyFast(payload, length, recvTime);

                    if (count >= expectedTotalRecv) {
                        allDeliveriesLatch.countDown();
                    }
                }
            } catch (IOException e) {
                if (running) totalErrors.incrementAndGet();
            }
        }

        @Override
        public void close() throws IOException {
            running = false;
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }

    // =========================================================================
    // UDP CLIENT (Datagram packet based with 4MB buffer)
    // =========================================================================
    private static class SimulatedUDPClient implements Runnable, AutoCloseable {
        private final int clientId;
        private final InetSocketAddress serverAddress;
        private final CountDownLatch connectLatch;
        private final CountDownLatch startSignal;
        private final CountDownLatch sendCompleteLatch;
        private final long expectedTotalRecv;

        private DatagramSocket socket;
        private volatile boolean running = true;

        public SimulatedUDPClient(int clientId,
                                  InetSocketAddress serverAddress,
                                  CountDownLatch connectLatch,
                                  CountDownLatch startSignal,
                                  CountDownLatch sendCompleteLatch,
                                  long expectedTotalRecv) {
            this.clientId = clientId;
            this.serverAddress = serverAddress;
            this.connectLatch = connectLatch;
            this.startSignal = startSignal;
            this.sendCompleteLatch = sendCompleteLatch;
            this.expectedTotalRecv = expectedTotalRecv;
        }

        @Override
        public void run() {
            try {
                socket = new DatagramSocket();
                socket.setReceiveBufferSize(4 * 1024 * 1024);
                socket.setSendBufferSize(4 * 1024 * 1024);

                Thread reader = new Thread(this::readLoop, "udp-fast-reader-" + clientId);
                reader.setDaemon(true);
                reader.start();

                // Send discovery handshake
                byte[] reg = ("[REGISTER]|C" + clientId + "\n").getBytes(StandardCharsets.UTF_8);
                socket.send(new DatagramPacket(reg, reg.length, serverAddress));

                connectLatch.countDown();
                startSignal.await();

                for (int seq = 1; seq <= MESSAGES_PER_CLIENT; seq++) {
                    long now = System.currentTimeMillis();
                    String msg = "[BENCHMARK]|C" + clientId + "|" + seq + "|" + now + "|msg_" + seq + "\n";
                    byte[] data = msg.getBytes(StandardCharsets.UTF_8);

                    socket.send(new DatagramPacket(data, data.length, serverAddress));

                    totalMessagesSent.incrementAndGet();
                    totalBytesSent.addAndGet(data.length);

                    if (SEND_INTERVAL_MS > 0) {
                        Thread.sleep(SEND_INTERVAL_MS);
                    }
                }
            } catch (Exception e) {
                totalErrors.incrementAndGet();
            } finally {
                sendCompleteLatch.countDown();
            }
        }

        private void readLoop() {
            byte[] buf = new byte[8192];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            while (running) {
                try {
                    socket.receive(packet);
                    long recvTime = System.currentTimeMillis();
                    int count = totalMessagesReceived.incrementAndGet();
                    totalBytesReceived.addAndGet(packet.getLength());

                    parseLatencyFast(packet.getData(), packet.getLength(), recvTime);

                    if (count >= expectedTotalRecv) {
                        allDeliveriesLatch.countDown();
                    }
                } catch (SocketException e) {
                    break;
                } catch (IOException e) {
                    if (running) totalErrors.incrementAndGet();
                }
            }
        }

        @Override
        public void close() {
            running = false;
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }

    /**
     * Ultra-fast zero-allocation timestamp parser directly from raw bytes.
     * Extracts sent timestamp between 3rd and 4th '|' delimiter without String allocations.
     */
    private static void parseLatencyFast(byte[] data, int length, long recvTime) {
        int pipeCount = 0;
        int tsStart = -1;
        int tsEnd = -1;

        for (int i = 0; i < length; i++) {
            if (data[i] == '|') {
                pipeCount++;
                if (pipeCount == 3) {
                    tsStart = i + 1;
                } else if (pipeCount == 4) {
                    tsEnd = i;
                    break;
                }
            }
        }

        if (tsStart != -1 && tsEnd > tsStart) {
            long sentTime = 0;
            for (int i = tsStart; i < tsEnd; i++) {
                byte b = data[i];
                if (b >= '0' && b <= '9') {
                    sentTime = sentTime * 10 + (b - '0');
                }
            }
            if (sentTime > 0) {
                long latency = Math.max(0, recvTime - sentTime);
                latenciesMs.add(latency);
            }
        }
    }
}

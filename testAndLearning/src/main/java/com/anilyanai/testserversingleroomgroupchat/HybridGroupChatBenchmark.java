package com.anilyanai.testserversingleroomgroupchat;

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
 * 🚀 HIGH-PERFORMANCE HYBRID CONCURRENT BENCHMARK (TCP + UDP BURST MODE)
 * =========================================================================================
 *
 * Runs concurrent TCP and UDP clients in full duplex:
 *   • All clients burst messages simultaneously across both protocols at 0ms interval.
 *   • Zero-allocation byte parsing for sub-millisecond latency tracking.
 *   • Dynamic idle detection for precise transmission timing (no hardcoded sleep).
 */
public class HybridGroupChatBenchmark {

    // =========================================================================
    // BENCHMARK CONFIGURATION
    // =========================================================================
    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    private static final int NUM_TCP_CLIENTS = 25;           // 25 Concurrent TCP clients
    private static final int NUM_UDP_CLIENTS = 25;           // 25 Concurrent UDP clients
    private static final int TOTAL_CLIENTS = NUM_TCP_CLIENTS + NUM_UDP_CLIENTS;

    private static final int MESSAGES_PER_CLIENT = 100;      // 100 messages per client
    private static final int SEND_INTERVAL_MS = 0;           // ⚡ 0 ms = Maximum burst speed
    private static final int WARMUP_DELAY_MS = 2000;         // Settle time for server registration
    private static final int IDLE_SETTLE_TIMEOUT_SEC = 2;    // Auto-finish if no new broadcasts arrive for 2 seconds
    private static final int SOCKET_BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB Socket Kernel Windows

    // =========================================================================
    // METRICS TRACKING
    // =========================================================================
    // TCP Metrics
    private static final AtomicInteger tcpMessagesSent = new AtomicInteger(0);
    private static final AtomicLong tcpMessagesReceived = new AtomicLong(0);
    private static final AtomicLong tcpBytesSent = new AtomicLong(0);
    private static final AtomicLong tcpBytesReceived = new AtomicLong(0);
    private static final AtomicInteger tcpErrors = new AtomicInteger(0);
    private static final ConcurrentLinkedQueue<Long> tcpLatenciesMs = new ConcurrentLinkedQueue<>();

    // UDP Metrics
    private static final AtomicInteger udpMessagesSent = new AtomicInteger(0);
    private static final AtomicLong udpMessagesReceived = new AtomicLong(0);
    private static final AtomicLong udpBytesSent = new AtomicLong(0);
    private static final AtomicLong udpBytesReceived = new AtomicLong(0);
    private static final AtomicInteger udpErrors = new AtomicInteger(0);
    private static final ConcurrentLinkedQueue<Long> udpLatenciesMs = new ConcurrentLinkedQueue<>();

    private static final AtomicBoolean isTestActive = new AtomicBoolean(false);
    private static final CountDownLatch allDeliveriesLatch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        int expectedTotalSent = TOTAL_CLIENTS * MESSAGES_PER_CLIENT;
        int fanOutMultiplier = TOTAL_CLIENTS - 1;
        long expectedTotalReceived = (long) expectedTotalSent * fanOutMultiplier;

        System.out.println("=================================================================");
        System.out.println("🔬 TRUE CONCURRENT HYBRID BENCHMARK (25 TCP + 25 UDP BURST MODE)");
        System.out.println("=================================================================");
        System.out.printf("Target Server            : %s:%d%n", HOST, PORT);
        System.out.printf("Total Clients            : %d (TCP: %d, UDP: %d)%n", TOTAL_CLIENTS, NUM_TCP_CLIENTS, NUM_UDP_CLIENTS);
        System.out.printf("Messages per Client      : %d (Total Inbound Sent: %,d)%n", MESSAGES_PER_CLIENT, expectedTotalSent);
        System.out.printf("Expected Broadcasts Recv : %,d (Fan-Out Multiplier: %d)%n", expectedTotalReceived, fanOutMultiplier);
        System.out.printf("Send Interval / Client   : %d ms (BURST SPEED with 128KB In / 64KB Out Buffers)%n", SEND_INTERVAL_MS);
        System.out.println("=================================================================\\n");

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        CountDownLatch tcpConnectLatch = new CountDownLatch(NUM_TCP_CLIENTS);
        CountDownLatch udpRegisterLatch = new CountDownLatch(NUM_UDP_CLIENTS);
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch allSendCompleteLatch = new CountDownLatch(TOTAL_CLIENTS);

        List<SimulatedTCPClient> tcpClients = new ArrayList<>();
        List<SimulatedUDPClient> udpClients = new ArrayList<>();
        InetSocketAddress serverAddress = new InetSocketAddress(HOST, PORT);

        // 1. Connect TCP Clients
        System.out.printf("⏳ Connecting %d TCP clients...%n", NUM_TCP_CLIENTS);
        for (int i = 1; i <= NUM_TCP_CLIENTS; i++) {
            SimulatedTCPClient client = new SimulatedTCPClient(i, tcpConnectLatch, startSignal, allSendCompleteLatch, expectedTotalReceived);
            tcpClients.add(client);
            executor.submit(client);
        }

        if (!tcpConnectLatch.await(15, TimeUnit.SECONDS)) {
            System.err.println("⚠️ Warning: Some TCP clients could not connect in time.");
        } else {
            System.out.println("✅ All " + NUM_TCP_CLIENTS + " TCP clients connected!");
        }

        // 2. Initialize UDP Clients and Send Registration
        System.out.printf("⏳ Registering %d UDP clients...%n", NUM_UDP_CLIENTS);
        for (int i = 1; i <= NUM_UDP_CLIENTS; i++) {
            SimulatedUDPClient client = new SimulatedUDPClient(i, serverAddress, udpRegisterLatch, startSignal, allSendCompleteLatch, expectedTotalReceived);
            udpClients.add(client);
            executor.submit(client);
        }

        if (!udpRegisterLatch.await(10, TimeUnit.SECONDS)) {
            System.err.println("⚠️ Warning: Some UDP clients could not register in time.");
        } else {
            System.out.println("✅ All " + NUM_UDP_CLIENTS + " UDP clients registered!");
        }

        System.out.printf("⏳ Warming up (%d ms) for server routing tables to settle...%n", WARMUP_DELAY_MS);
        Thread.sleep(WARMUP_DELAY_MS);

        // Reset metrics after warmup handshake pings
        tcpMessagesReceived.set(0);
        udpMessagesReceived.set(0);
        tcpBytesReceived.set(0);
        udpBytesReceived.set(0);

        // Start Live Reporter
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        long benchmarkStartTime = System.currentTimeMillis();
        isTestActive.set(true);

        reporter.scheduleAtFixedRate(() -> {
            if (!isTestActive.get()) return;
            long elapsedSec = Math.max(1, (System.currentTimeMillis() - benchmarkStartTime) / 1000);
            int sentTcp = tcpMessagesSent.get();
            int sentUdp = udpMessagesSent.get();
            long recvTcp = tcpMessagesReceived.get();
            long recvUdp = udpMessagesReceived.get();
            long totalRecv = recvTcp + recvUdp;
            double progress = expectedTotalReceived > 0 ? ((double) totalRecv / expectedTotalReceived) * 100.0 : 0.0;

            System.out.printf("[⏱️  %2ds] Sent: %,5d (TCP: %,d, UDP: %,d) | Recv: %,7d / %,7d (%.1f%%) | Err: %d%n",
                    elapsedSec, (sentTcp + sentUdp), sentTcp, sentUdp, totalRecv, expectedTotalReceived, progress,
                    (tcpErrors.get() + udpErrors.get()));
        }, 1, 1, TimeUnit.SECONDS);

        // =========================================================================
        // 🔥 SIMULTANEOUS CONCURRENT EXECUTION
        // =========================================================================
        System.out.println("\\n-----------------------------------------------------------------");
        System.out.println("🔥 FIRING ALL 50 CLIENTS SIMULTANEOUSLY AT FULL BURST SPEED...");
        System.out.println("-----------------------------------------------------------------\\n");

        startSignal.countDown(); // Release all 50 threads simultaneously

        // Wait for all clients to finish sending their messages
        allSendCompleteLatch.await();
        long sendEndTime = System.currentTimeMillis();
        double sendDurationSec = (sendEndTime - benchmarkStartTime) / 1000.0;

        System.out.printf("📤 [Phase 1: Inbound] All clients finished sending in %.2f seconds (%,.2f msg/sec).%n",
                sendDurationSec, expectedTotalSent / Math.max(0.001, sendDurationSec));
        System.out.printf("⏳ [Phase 2: Outbound] Waiting for server to finish broadcasting all %,d messages...%n", expectedTotalReceived);

        // Dynamic wait with idle detection
        long lastRecvCount = 0;
        int idleSeconds = 0;
        long broadcastEndTime = System.currentTimeMillis();

        while (isTestActive.get()) {
            boolean allDone = allDeliveriesLatch.await(1, TimeUnit.SECONDS);
            long currentRecv = tcpMessagesReceived.get() + udpMessagesReceived.get();

            if (allDone || currentRecv >= expectedTotalReceived) {
                broadcastEndTime = System.currentTimeMillis();
                System.out.printf("🎉 All %,d broadcast messages fully delivered and received!%n", currentRecv);
                break;
            }

            if (currentRecv == lastRecvCount && currentRecv > 0) {
                idleSeconds++;
                if (idleSeconds >= IDLE_SETTLE_TIMEOUT_SEC) {
                    broadcastEndTime = System.currentTimeMillis() - (IDLE_SETTLE_TIMEOUT_SEC * 1000L);
                    System.out.println("\\n✅ Traffic settled. Finalizing benchmark report...");
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
        for (SimulatedTCPClient client : tcpClients) {
            client.close();
        }
        for (SimulatedUDPClient client : udpClients) {
            client.close();
        }
        executor.shutdownNow();

        // =========================================================================
        // 📊 RESULTS AND PERFORMANCE REPORT
        // =========================================================================
        printPerformanceSummary(expectedTotalSent, expectedTotalReceived, sendDurationSec, broadcastDurationSec);
    }

    private static void printPerformanceSummary(int expectedSent, long expectedReceived, double sendDurationSec, double broadcastDurationSec) {
        int totalSentTcp = tcpMessagesSent.get();
        int totalSentUdp = udpMessagesSent.get();
        int totalSent = totalSentTcp + totalSentUdp;

        long totalRecvTcp = tcpMessagesReceived.get();
        long totalRecvUdp = udpMessagesReceived.get();
        long totalRecv = totalRecvTcp + totalRecvUdp;

        long totalBytesOut = tcpBytesSent.get() + udpBytesSent.get();
        long totalBytesIn = tcpBytesReceived.get() + udpBytesReceived.get();

        double clientThroughputMsgsSec = totalSent / Math.max(0.001, sendDurationSec);
        double serverBroadcastRateMsgsSec = totalRecv / Math.max(0.001, broadcastDurationSec);
        double deliveryRatio = expectedReceived > 0 ? ((double) totalRecv / expectedReceived) * 100.0 : 0.0;

        System.out.println("\\n=================================================================");
        System.out.println("🏁 PERFORMANCE BENCHMARK REPORT (50 CLIENTS x 100 MSGS)");
        System.out.println("=================================================================");
        System.out.printf("Total Messages Sent (Inbound)    : %,d (Expected: %,d)%n", totalSent, expectedSent);
        System.out.printf("Total Broadcasts Recv (Outbound) : %,d (Expected: %,d)%n", totalRecv, expectedReceived);
        System.out.printf("Delivery Success Rate            : %.2f%%%n", deliveryRatio);
        System.out.printf("Total Data Transferred           : Inbound: %.2f MB | Outbound: %.2f MB%n",
                totalBytesOut / (1024.0 * 1024.0), totalBytesIn / (1024.0 * 1024.0));
        System.out.printf("Total Errors Encountered         : TCP: %d | UDP: %d%n", tcpErrors.get(), udpErrors.get());
        System.out.println("-----------------------------------------------------------------");
        System.out.println("⏱️  PHASE TIMING & THROUGHPUT BREAKDOWN:");
        System.out.printf("  1. Client -> Server Inbound    : %.2f seconds (Throughput: %,.2f msg/sec)%n",
                sendDurationSec, clientThroughputMsgsSec);
        System.out.printf("  2. Server -> Client Broadcast  : %.2f seconds (Throughput: %,.2f msg/sec)%n",
                broadcastDurationSec, serverBroadcastRateMsgsSec);
        System.out.println("-----------------------------------------------------------------");

        System.out.println("\\n--- 📈 TCP LATENCY BREAKDOWN (ms) ---");
        printLatencyStats(tcpLatenciesMs);

        System.out.println("\\n--- 📈 UDP LATENCY BREAKDOWN (ms) ---");
        printLatencyStats(udpLatenciesMs);
        System.out.println("=================================================================\\n");
    }

    private static void printLatencyStats(ConcurrentLinkedQueue<Long> latencies) {
        if (latencies.isEmpty()) {
            System.out.println("  No latency data recorded.");
            return;
        }

        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        int size = sorted.size();

        long min = sorted.getFirst();
        long max = sorted.getLast();
        double sum = 0;
        for (long val : sorted) sum += val;
        double avg = sum / size;

        long p50 = sorted.get((int) (size * 0.50));
        long p90 = sorted.get((int) (size * 0.90));
        long p95 = sorted.get((int) (size * 0.95));
        long p99 = sorted.get((int) (size * 0.99));

        System.out.printf("  Samples: %,d | Min: %d ms | Avg: %.2f ms | Max: %d ms%n", size, min, avg, max);
        System.out.printf("  Percentiles: P50: %d ms | P90: %d ms | P95: %d ms | P99: %d ms%n", p50, p90, p95, p99);
    }

    /**
     * Simulated TCP Client Worker using Length-Prefixed Framing (4-byte length header)
     */
    private static class SimulatedTCPClient implements Runnable {
        private final int clientId;
        private final CountDownLatch connectLatch;
        private final CountDownLatch startSignal;
        private final CountDownLatch allSendCompleteLatch;
        private final long expectedTotalRecv;

        private Socket socket;
        private DataInputStream in;
        private DataOutputStream out;
        private volatile boolean running = true;

        public SimulatedTCPClient(int clientId,
                                  CountDownLatch connectLatch,
                                  CountDownLatch startSignal,
                                  CountDownLatch allSendCompleteLatch,
                                  long expectedTotalRecv) {
            this.clientId = clientId;
            this.connectLatch = connectLatch;
            this.startSignal = startSignal;
            this.allSendCompleteLatch = allSendCompleteLatch;
            this.expectedTotalRecv = expectedTotalRecv;
        }

        @Override
        public void run() {
            try {
                socket = new Socket(HOST, PORT);
                socket.setTcpNoDelay(true);
                socket.setReceiveBufferSize(SOCKET_BUFFER_SIZE);
                socket.setSendBufferSize(SOCKET_BUFFER_SIZE);

                in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 128 * 1024));
                out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));

                Thread.ofVirtual().name("tcp-reader-" + clientId).start(this::readLoop);

                // Initial handshake registration
                byte[] reg = ("[REGISTER]|TCP_C" + clientId).getBytes(StandardCharsets.UTF_8);
                out.writeInt(reg.length);
                out.write(reg);
                out.flush();

                connectLatch.countDown();
                startSignal.await();

                for (int seq = 1; seq <= MESSAGES_PER_CLIENT; seq++) {
                    long now = System.currentTimeMillis();
                    String payloadStr = "[BENCHMARK]|TCP_C" + clientId + "|" + seq + "|" + now + "|msg_" + seq;
                    byte[] payload = payloadStr.getBytes(StandardCharsets.UTF_8);

                    out.writeInt(payload.length);
                    out.write(payload);

                    tcpMessagesSent.incrementAndGet();
                    tcpBytesSent.addAndGet(4 + payload.length);

                    if (SEND_INTERVAL_MS > 0) {
                        Thread.sleep(SEND_INTERVAL_MS);
                    }
                }
                out.flush(); // Batch flush all messages at wire speed

            } catch (Exception e) {
                tcpErrors.incrementAndGet();
            } finally {
                allSendCompleteLatch.countDown();
            }
        }

        private void readLoop() {
            try {
                byte[] readBuffer = new byte[8192];
                while (running) {
                    int frameLength = in.readInt();
                    if (frameLength < 0 || frameLength > 16 * 1024 * 1024) break;

                    byte[] payload = (frameLength <= readBuffer.length) ? readBuffer : new byte[frameLength];
                    in.readFully(payload, 0, frameLength);

                    long recvTime = System.currentTimeMillis();
                    long count = tcpMessagesReceived.incrementAndGet() + udpMessagesReceived.get();
                    tcpBytesReceived.addAndGet(4 + frameLength);

                    parseLatencyFast(payload, frameLength, recvTime, tcpLatenciesMs);

                    if (count >= expectedTotalRecv) {
                        allDeliveriesLatch.countDown();
                    }
                }
            } catch (EOFException | SocketException e) {
                // Socket closed normally during teardown
            } catch (IOException e) {
                if (running) {
                    tcpErrors.incrementAndGet();
                }
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

    /**
     * Simulated UDP Client Worker
     */
    private static class SimulatedUDPClient implements Runnable {
        private final int clientId;
        private final InetSocketAddress serverAddress;
        private final CountDownLatch registerLatch;
        private final CountDownLatch startSignal;
        private final CountDownLatch allSendCompleteLatch;
        private final long expectedTotalRecv;

        private DatagramSocket socket;
        private volatile boolean running = true;

        public SimulatedUDPClient(int clientId,
                                  InetSocketAddress serverAddress,
                                  CountDownLatch registerLatch,
                                  CountDownLatch startSignal,
                                  CountDownLatch allSendCompleteLatch,
                                  long expectedTotalRecv) {
            this.clientId = clientId;
            this.serverAddress = serverAddress;
            this.registerLatch = registerLatch;
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

                Thread.ofVirtual().name("udp-reader-" + clientId).start(this::readLoop);

                // Initial UDP registration handshake
                byte[] regPayload = ("[REGISTER]|UDP_C" + clientId + "\n").getBytes(StandardCharsets.UTF_8);
                DatagramPacket regPacket = new DatagramPacket(regPayload, regPayload.length, serverAddress);
                socket.send(regPacket);

                registerLatch.countDown();
                startSignal.await();

                for (int seq = 1; seq <= MESSAGES_PER_CLIENT; seq++) {
                    long now = System.currentTimeMillis();
                    String message = "[BENCHMARK]|UDP_C" + clientId + "|" + seq + "|" + now + "|msg_" + seq + "\n";
                    byte[] data = message.getBytes(StandardCharsets.UTF_8);

                    DatagramPacket packet = new DatagramPacket(data, data.length, serverAddress);
                    socket.send(packet);

                    udpMessagesSent.incrementAndGet();
                    udpBytesSent.addAndGet(data.length);

                    if (SEND_INTERVAL_MS > 0) {
                        Thread.sleep(SEND_INTERVAL_MS);
                    }
                }

            } catch (Exception e) {
                udpErrors.incrementAndGet();
            } finally {
                allSendCompleteLatch.countDown();
            }
        }

        private void readLoop() {
            byte[] buf = new byte[8192];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            while (running) {
                try {
                    socket.receive(packet);
                    long recvTime = System.currentTimeMillis();
                    long count = tcpMessagesReceived.get() + udpMessagesReceived.incrementAndGet();
                    udpBytesReceived.addAndGet(packet.getLength());

                    parseLatencyFast(packet.getData(), packet.getLength(), recvTime, udpLatenciesMs);

                    if (count >= expectedTotalRecv) {
                        allDeliveriesLatch.countDown();
                    }
                } catch (SocketException e) {
                    break;
                } catch (IOException e) {
                    if (running) {
                        udpErrors.incrementAndGet();
                    }
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

    /**
     * Zero-allocation fast byte-level timestamp parser.
     */
    private static void parseLatencyFast(byte[] data, int length, long recvTime, ConcurrentLinkedQueue<Long> latencies) {
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
                latencies.add(latency);
            }
        }
    }
}

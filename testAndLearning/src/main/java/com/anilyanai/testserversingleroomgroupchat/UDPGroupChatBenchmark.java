package com.anilyanai.testserversingleroomgroupchat;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * =========================================================================================
 * 📊 HIGH-CONCURRENCY BENCHMARK: Single Room Group Chat Server (UDP Protocol)
 * =========================================================================================
 *
 * 📐 THE MATH BEHIND THE UDP LOAD:
 * -----------------------------------------------------------------------------------------
 * 1. Per-Client Message Rate:
 *      Rate = 1000 ms / SEND_INTERVAL_MS
 *      Example: 1000 ms / 50 ms = 20 datagram packets / second per client.
 *
 * 2. Total Inbound Traffic (Server Inbound):
 *      Inbound Msg/Sec = NUM_CLIENTS * (1000 / SEND_INTERVAL_MS)
 *      Example: 50 clients * 20 msg/sec = 1,000 UDP packets / second sent to server.
 *
 * 3. Total Broadcast Fan-Out (Server Outbound):
 *      In single-room chat, each incoming datagram is broadcast to all OTHER clients (N - 1).
 *      Fan-Out Multiplier = (NUM_CLIENTS - 1)
 *      Outbound Msg/Sec   = Inbound Msg/Sec * (NUM_CLIENTS - 1)
 *      Example: 1,000 msg/sec * 49 peers = 49,000 UDP datagrams / second!
 *
 * 4. Total Messages Exchanged During Test:
 *      - Total Inbound Packets   = NUM_CLIENTS * MESSAGES_PER_CLIENT
 *                                = 50 * 200 = 10,000 UDP packets.
 *      - Total Expected Outbound = Total Inbound Packets * (NUM_CLIENTS - 1)
 *                                = 10,000 * 49 = 490,000 broadcast UDP packets.
 *
 * 5. Test Duration:
 *      Sending Duration ≈ (MESSAGES_PER_CLIENT * SEND_INTERVAL_MS) / 1000
 *                       ≈ (200 * 50 ms) / 1000 = ~10 seconds (+ settling time).
 * -----------------------------------------------------------------------------------------
 */
public class UDPGroupChatBenchmark {

    // =========================================================================
    // BENCHMARK CONFIGURATION
    // =========================================================================
    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    // Load settings:
    private static final int NUM_CLIENTS = 50;               // 50 concurrent UDP clients
    private static final int MESSAGES_PER_CLIENT = 200;      // 200 datagrams per client
    private static final int SEND_INTERVAL_MS = 50;          // 50 ms between sends (~20 msg/s per client)
    private static final int WARMUP_DELAY_MS = 2000;         // Settle time for server UDP address discovery
    private static final int SOCKET_BUFFER_SIZE = 1024 * 1024; // 1 MB client OS socket receive buffer

    // =========================================================================
    // METRICS TRACKING
    // =========================================================================
    private static final AtomicInteger totalMessagesSent = new AtomicInteger(0);
    private static final AtomicLong totalMessagesReceived = new AtomicLong(0);
    private static final AtomicInteger connectionErrors = new AtomicInteger(0);
    private static final AtomicLong totalBytesSent = new AtomicLong(0);
    private static final AtomicLong totalBytesReceived = new AtomicLong(0);
    private static final ConcurrentLinkedQueue<Long> latenciesMs = new ConcurrentLinkedQueue<>();
    private static final AtomicBoolean isTestActive = new AtomicBoolean(false);

    public static void main(String[] args) throws Exception {
        int expectedTotalSent = NUM_CLIENTS * MESSAGES_PER_CLIENT;
        int expectedPerMessage = NUM_CLIENTS - 1;
        long expectedTotalReceived = (long) expectedTotalSent * expectedPerMessage;
        double estimatedSendTimeSec = (MESSAGES_PER_CLIENT * SEND_INTERVAL_MS) / 1000.0;

        System.out.println("=================================================================");
        System.out.println("🚀 UDP SINGLE ROOM GROUP CHAT HIGH-CONCURRENCY BENCHMARK");
        System.out.println("=================================================================");
        System.out.printf("Target Server            : %s:%d (UDP)%n", HOST, PORT);
        System.out.printf("Concurrent Clients       : %d%n", NUM_CLIENTS);
        System.out.printf("Messages per Client      : %d%n", MESSAGES_PER_CLIENT);
        System.out.printf("Send Interval / Client   : %d ms (~%.1f msg/s per client)%n",
                SEND_INTERVAL_MS, 1000.0 / Math.max(1, SEND_INTERVAL_MS));
        System.out.printf("Estimated Sending Time   : ~%.1f seconds%n", estimatedSendTimeSec);
        System.out.printf("Total Messages to Send   : %,d%n", expectedTotalSent);
        System.out.printf("Expected Broadcasts Recv : %,d (each packet broadcast to %,d clients)%n",
                expectedTotalReceived, expectedPerMessage);
        System.out.println("=================================================================\n");

        ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch registerLatch = new CountDownLatch(NUM_CLIENTS);
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch sendCompleteLatch = new CountDownLatch(NUM_CLIENTS);
        List<SimulatedUDPClient> clients = new ArrayList<>();
        InetSocketAddress serverAddress = new InetSocketAddress(HOST, PORT);

        // 1. Initialize UDP clients & send discovery handshake
        System.out.printf("⏳ Initializing %d UDP clients and sending registration handshake...%n", NUM_CLIENTS);
        for (int i = 1; i <= NUM_CLIENTS; i++) {
            SimulatedUDPClient client = new SimulatedUDPClient(i, serverAddress, registerLatch, startSignal, sendCompleteLatch);
            clients.add(client);
            virtualExecutor.submit(client);
        }

        if (!registerLatch.await(10, TimeUnit.SECONDS)) {
            System.err.println("⚠️ Warning: Some UDP clients could not register in time.");
        } else {
            System.out.println("✅ All " + NUM_CLIENTS + " UDP clients registered with server!");
        }

        System.out.printf("⏳ Waiting %,d ms for server UDP routing table to settle...%n", WARMUP_DELAY_MS);
        Thread.sleep(WARMUP_DELAY_MS);

        // 2. Start Live Progress Reporter
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        long benchmarkStartTime = System.currentTimeMillis();
        isTestActive.set(true);

        reporter.scheduleAtFixedRate(() -> {
            if (!isTestActive.get()) return;
            long elapsedSec = Math.max(1, (System.currentTimeMillis() - benchmarkStartTime) / 1000);
            int sent = totalMessagesSent.get();
            long recv = totalMessagesReceived.get();
            System.out.printf("[⏱️ %2ds] Sent: %,6d / %,6d | Recv UDP Broadcasts: %,8d | Errors: %d%n",
                    elapsedSec, sent, expectedTotalSent, recv, connectionErrors.get());
        }, 2, 2, TimeUnit.SECONDS);

        // 3. Fire benchmark traffic
        System.out.println("\n🔥 Firing UDP traffic across all 50 clients simultaneously...\n");
        startSignal.countDown();

        // Wait for all clients to finish sending
        sendCompleteLatch.await();
        long sendingEndTime = System.currentTimeMillis();
        System.out.printf("%n📤 All %d clients completed sending in %.2f seconds.%n",
                NUM_CLIENTS, (sendingEndTime - benchmarkStartTime) / 1000.0);

        // 4. In-flight settling window
        System.out.println("⏳ Collecting remaining in-flight UDP datagram deliveries...");
        Thread.sleep(3000);
        isTestActive.set(false);
        reporter.shutdown();
        long benchmarkEndTime = System.currentTimeMillis();

        // 5. Cleanup
        for (SimulatedUDPClient client : clients) {
            client.close();
        }
        virtualExecutor.shutdownNow();

        // 6. Generate final report
        printBenchmarkReport(benchmarkStartTime, benchmarkEndTime, expectedTotalReceived, expectedTotalSent);
    }

    private static void printBenchmarkReport(long startTime, long endTime, long expectedTotalReceived, int expectedTotalSent) {
        long totalDurationMs = Math.max(1, endTime - startTime);
        double totalDurationSec = totalDurationMs / 1000.0;
        int sent = totalMessagesSent.get();
        long received = totalMessagesReceived.get();
        int errors = connectionErrors.get();

        List<Long> latencyList = new ArrayList<>(latenciesMs);
        Collections.sort(latencyList);

        double sendThroughput = sent / totalDurationSec;
        double recvThroughput = received / totalDurationSec;
        double deliveryRatio = expectedTotalReceived > 0 ? ((double) received / expectedTotalReceived) * 100.0 : 0;
        long droppedPackets = Math.max(0, expectedTotalReceived - received);

        System.out.println("\n=================================================================");
        System.out.println("📊 FINAL UDP BENCHMARK PERFORMANCE REPORT");
        System.out.println("=================================================================");
        System.out.printf("Total Test Duration      : %.2f seconds (%d ms)%n", totalDurationSec, totalDurationMs);
        System.out.printf("Concurrent UDP Clients   : %d%n", NUM_CLIENTS);
        System.out.printf("Socket / Network Errors  : %d%n", errors);
        System.out.println("-----------------------------------------------------------------");
        System.out.printf("Total Datagrams Sent     : %,d / %,d%n", sent, expectedTotalSent);
        System.out.printf("Expected Broadcasts Recv : %,d%n", expectedTotalReceived);
        System.out.printf("Actual Broadcasts Recv   : %,d%n", received);
        System.out.printf("Dropped / Lost Packets   : %,d%n", droppedPackets);
        System.out.printf("Delivery Success Rate    : %.2f%%%n", deliveryRatio);
        System.out.println("-----------------------------------------------------------------");
        System.out.printf("UDP Send Rate (Inbound)  : %,.2f msgs/sec (%.2f KB/sec)%n",
                sendThroughput, (totalBytesSent.get() / 1024.0) / totalDurationSec);
        System.out.printf("UDP Broadcast Delivery   : %,.2f msgs/sec (%.2f KB/sec)%n",
                recvThroughput, (totalBytesReceived.get() / 1024.0) / totalDurationSec);
        System.out.println("-----------------------------------------------------------------");

        if (!latencyList.isEmpty()) {
            long min = latencyList.get(0);
            long max = latencyList.get(latencyList.size() - 1);
            double avg = latencyList.stream().mapToLong(Long::longValue).average().orElse(0.0);
            long p50 = percentile(latencyList, 50);
            long p90 = percentile(latencyList, 90);
            long p95 = percentile(latencyList, 95);
            long p99 = percentile(latencyList, 99);

            System.out.println("⏱️ End-to-End UDP Broadcast Latency (Round-Trip / Delivery):");
            System.out.printf("  Min Latency            : %d ms%n", min);
            System.out.printf("  Avg Latency            : %.2f ms%n", avg);
            System.out.printf("  p50 (Median)           : %d ms%n", p50);
            System.out.printf("  p90                    : %d ms%n", p90);
            System.out.printf("  p95                    : %d ms%n", p95);
            System.out.printf("  p99                    : %d ms%n", p99);
            System.out.printf("  Max Latency            : %d ms%n", max);
        } else {
            System.out.println("⚠️ No latency metrics collected (check server UDP broadcast delivery).");
        }
        System.out.println("=================================================================\n");
    }

    private static long percentile(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) return 0;
        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    /**
     * Simulated UDP Client Worker
     */
    private static class SimulatedUDPClient implements Runnable {
        private final int clientId;
        private final InetSocketAddress serverAddress;
        private final CountDownLatch registerLatch;
        private final CountDownLatch startSignal;
        private final CountDownLatch sendCompleteLatch;

        private DatagramSocket socket;
        private volatile boolean running = true;

        public SimulatedUDPClient(int clientId,
                                  InetSocketAddress serverAddress,
                                  CountDownLatch registerLatch,
                                  CountDownLatch startSignal,
                                  CountDownLatch sendCompleteLatch) {
            this.clientId = clientId;
            this.serverAddress = serverAddress;
            this.registerLatch = registerLatch;
            this.startSignal = startSignal;
            this.sendCompleteLatch = sendCompleteLatch;
        }

        @Override
        public void run() {
            try {
                socket = new DatagramSocket();
                socket.setReceiveBufferSize(SOCKET_BUFFER_SIZE);

                // Start background UDP receiver thread
                Thread.ofVirtual().name("udp-reader-" + clientId).start(this::readLoop);

                // Send initial registration packet so the server registers this client's IP + Port
                byte[] regPayload = String.format("[REGISTER]|C%d\n", clientId).getBytes(StandardCharsets.UTF_8);
                DatagramPacket regPacket = new DatagramPacket(regPayload, regPayload.length, serverAddress);
                socket.send(regPacket);

                registerLatch.countDown();

                // Wait for synchronized benchmark start
                startSignal.await();

                for (int seq = 1; seq <= MESSAGES_PER_CLIENT; seq++) {
                    long now = System.currentTimeMillis();
                    String message = String.format("[BENCHMARK]|C%d|%d|%d|msg_%d\n", clientId, seq, now, seq);
                    byte[] data = message.getBytes(StandardCharsets.UTF_8);

                    DatagramPacket packet = new DatagramPacket(data, data.length, serverAddress);
                    socket.send(packet);

                    totalMessagesSent.incrementAndGet();
                    totalBytesSent.addAndGet(data.length);

                    if (SEND_INTERVAL_MS > 0) {
                        Thread.sleep(SEND_INTERVAL_MS);
                    }
                }

            } catch (Exception e) {
                connectionErrors.incrementAndGet();
            } finally {
                sendCompleteLatch.countDown();
            }
        }

        private void readLoop() {
            byte[] buffer = new byte[4096];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            while (running) {
                try {
                    socket.receive(packet);
                    long recvTime = System.currentTimeMillis();
                    totalMessagesReceived.incrementAndGet();
                    totalBytesReceived.addAndGet(packet.getLength());

                    String line = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);

                    if (line.startsWith("[BENCHMARK]|")) {
                        String[] parts = line.trim().split("\\|");
                        if (parts.length >= 4) {
                            try {
                                long sentTime = Long.parseLong(parts[3]);
                                long latency = Math.max(0, recvTime - sentTime);
                                latenciesMs.add(latency);
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }
                } catch (SocketException e) {
                    // Socket closed on shutdown
                    break;
                } catch (IOException e) {
                    if (running) {
                        connectionErrors.incrementAndGet();
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
}

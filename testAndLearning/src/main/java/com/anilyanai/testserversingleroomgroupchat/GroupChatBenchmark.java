package com.anilyanai.testserversingleroomgroupchat;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * =========================================================================================
 * 📊 HIGH-CONCURRENCY BENCHMARK: Single Room Group Chat Server
 * =========================================================================================
 *
 * 📐 THE MATH BEHIND THE LOAD:
 * -----------------------------------------------------------------------------------------
 * 1. Per-Client Message Rate:
 *      Rate = 1000 ms / SEND_INTERVAL_MS
 *      Example: 1000 ms / 50 ms = 20 messages / second per client.
 *
 * 2. Total Inbound Traffic (Server Inbound):
 *      Inbound Msg/Sec = NUM_CLIENTS * (1000 / SEND_INTERVAL_MS)
 *      Example: 50 clients * 20 msg/sec = 1,000 messages / second sent to server.
 *
 * 3. Total Broadcast Fan-Out (Server Outbound):
 *      In single-room chat, each incoming message is broadcast to all OTHER clients (N - 1).
 *      Fan-Out Multiplier = (NUM_CLIENTS - 1)
 *      Outbound Msg/Sec   = Inbound Msg/Sec * (NUM_CLIENTS - 1)
 *      Example: 1,000 msg/sec * 49 peers = 49,000 broadcast messages / second!
 *
 * 4. Total Messages Exchanged During Test:
 *      - Total Inbound Messages  = NUM_CLIENTS * MESSAGES_PER_CLIENT
 *                                = 50 * 200 = 10,000 messages.
 *      - Total Expected Outbound = Total Inbound Messages * (NUM_CLIENTS - 1)
 *                                = 10,000 * 49 = 490,000 broadcast messages.
 *
 * 5. Test Duration:
 *      Sending Duration ≈ (MESSAGES_PER_CLIENT * SEND_INTERVAL_MS) / 1000
 *                       ≈ (200 * 50 ms) / 1000 = ~10 seconds (+ settling time).
 * -----------------------------------------------------------------------------------------
 */
public class GroupChatBenchmark {

    // =========================================================================
    // BENCHMARK CONFIGURATION (Adjust these to control load & test duration)
    // =========================================================================
    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    // Load settings:
    private static final int NUM_CLIENTS = 50;               // Concurrent connected clients (e.g., 20, 50, 100)
    private static final int MESSAGES_PER_CLIENT = 200;      // Total messages each client sends
    private static final int SEND_INTERVAL_MS = 50;          // Delay between sends per client (50ms = 20 msgs/s)
    private static final int WARMUP_DELAY_MS = 2000;         // Settle time after connecting before starting traffic

    // =========================================================================
    // METRICS TRACKING
    // =========================================================================
    private static final AtomicInteger totalMessagesSent = new AtomicInteger(0);
    private static final AtomicInteger totalMessagesReceived = new AtomicInteger(0);
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
        System.out.println("🚀 SINGLE ROOM GROUP CHAT SUSTAINED BENCHMARK");
        System.out.println("=================================================================");
        System.out.printf("Target Server            : %s:%d%n", HOST, PORT);
        System.out.printf("Concurrent Clients       : %d%n", NUM_CLIENTS);
        System.out.printf("Messages per Client      : %d%n", MESSAGES_PER_CLIENT);
        System.out.printf("Send Interval / Client   : %d ms (~%.1f msg/s per client)%n",
                SEND_INTERVAL_MS, 1000.0 / Math.max(1, SEND_INTERVAL_MS));
        System.out.printf("Estimated Sending Time   : ~%.1f seconds%n", estimatedSendTimeSec);
        System.out.printf("Total Messages to Send   : %,d%n", expectedTotalSent);
        System.out.printf("Expected Broadcasts Recv : %,d (each msg broadcast to %,d clients)%n",
                expectedTotalReceived, expectedPerMessage);
        System.out.println("=================================================================\n");

        ExecutorService clientExecutor = Executors.newFixedThreadPool(NUM_CLIENTS);
        CountDownLatch connectLatch = new CountDownLatch(NUM_CLIENTS);
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch sendCompleteLatch = new CountDownLatch(NUM_CLIENTS);
        List<SimulatedClient> clients = new ArrayList<>();

        // 1. Establish connections
        System.out.printf("⏳ Connecting %d clients to %s:%d...%n", NUM_CLIENTS, HOST, PORT);
        for (int i = 1; i <= NUM_CLIENTS; i++) {
            SimulatedClient client = new SimulatedClient(i, connectLatch, startSignal, sendCompleteLatch);
            clients.add(client);
            clientExecutor.submit(client);
        }

        if (!connectLatch.await(15, TimeUnit.SECONDS)) {
            System.err.println("❌ Warning: Some clients could not connect in time.");
        } else {
            System.out.println("✅ All " + NUM_CLIENTS + " clients connected!");
        }

        System.out.printf("⏳ Warming up (%d ms) for server registry to settle...%n", WARMUP_DELAY_MS);
        Thread.sleep(WARMUP_DELAY_MS);

        // 2. Start Live Progress Reporter Thread
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        long benchmarkStartTime = System.currentTimeMillis();
        isTestActive.set(true);

        reporter.scheduleAtFixedRate(() -> {
            if (!isTestActive.get()) return;
            long elapsedSec = Math.max(1, (System.currentTimeMillis() - benchmarkStartTime) / 1000);
            int sent = totalMessagesSent.get();
            int recv = totalMessagesReceived.get();
            System.out.printf("[⏱️ %2ds] Sent: %,6d / %,6d | Recv Broadcasts: %,8d | Errors: %d%n",
                    elapsedSec, sent, expectedTotalSent, recv, connectionErrors.get());
        }, 2, 2, TimeUnit.SECONDS);

        // 3. Fire benchmark traffic
        System.out.println("\n🔥 Starting high-concurrency traffic...\n");
        startSignal.countDown();

        // Wait for all clients to finish sending
        sendCompleteLatch.await();
        long sendingEndTime = System.currentTimeMillis();
        System.out.printf("%n📤 All %d clients completed sending in %.2f seconds.%n",
                NUM_CLIENTS, (sendingEndTime - benchmarkStartTime) / 1000.0);

        // 4. In-flight settling window
        System.out.println("⏳ Collecting remaining in-flight broadcast deliveries...");
        Thread.sleep(3000);
        isTestActive.set(false);
        reporter.shutdown();
        long benchmarkEndTime = System.currentTimeMillis();

        // 5. Cleanup connections
        for (SimulatedClient client : clients) {
            client.close();
        }
        clientExecutor.shutdownNow();

        // 6. Generate final report
        printBenchmarkReport(benchmarkStartTime, benchmarkEndTime, expectedTotalReceived);
    }

    private static void printBenchmarkReport(long startTime, long endTime, long expectedTotalReceived) {
        long totalDurationMs = Math.max(1, endTime - startTime);
        double totalDurationSec = totalDurationMs / 1000.0;
        int sent = totalMessagesSent.get();
        int received = totalMessagesReceived.get();
        int errors = connectionErrors.get();

        List<Long> latencyList = new ArrayList<>(latenciesMs);
        Collections.sort(latencyList);

        double sendThroughput = sent / totalDurationSec;
        double recvThroughput = received / totalDurationSec;
        double deliveryRatio = expectedTotalReceived > 0 ? ((double) received / expectedTotalReceived) * 100.0 : 0;

        System.out.println("\n=================================================================");
        System.out.println("📊 FINAL BENCHMARK PERFORMANCE REPORT");
        System.out.println("=================================================================");
        System.out.printf("Total Test Duration      : %.2f seconds (%d ms)%n", totalDurationSec, totalDurationMs);
        System.out.printf("Concurrent Clients       : %d%n", NUM_CLIENTS);
        System.out.printf("Connection / Socket Err  : %d%n", errors);
        System.out.println("-----------------------------------------------------------------");
        System.out.printf("Total Messages Sent      : %,d%n", sent);
        System.out.printf("Expected Broadcasts Recv : %,d%n", expectedTotalReceived);
        System.out.printf("Actual Broadcasts Recv   : %,d%n", received);
        System.out.printf("Delivery Success Rate    : %.2f%%%n", deliveryRatio);
        System.out.println("-----------------------------------------------------------------");
        System.out.printf("Send Rate (Publisher)    : %,.2f msgs/sec (%.2f KB/sec)%n",
                sendThroughput, (totalBytesSent.get() / 1024.0) / totalDurationSec);
        System.out.printf("Broadcast Delivery Rate  : %,.2f msgs/sec (%.2f KB/sec)%n",
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

            System.out.println("⏱️ End-to-End Broadcast Latency (Round-Trip / Delivery):");
            System.out.printf("  Min Latency            : %d ms%n", min);
            System.out.printf("  Avg Latency            : %.2f ms%n", avg);
            System.out.printf("  p50 (Median)           : %d ms%n", p50);
            System.out.printf("  p90                    : %d ms%n", p90);
            System.out.printf("  p95                    : %d ms%n", p95);
            System.out.printf("  p99                    : %d ms%n", p99);
            System.out.printf("  Max Latency            : %d ms%n", max);
        } else {
            System.out.println("⚠️ No latency metrics collected (check server broadcast delivery).");
        }
        System.out.println("=================================================================\n");
    }

    private static long percentile(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) return 0;
        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    /**
     * Simulated Client Worker
     */
    private static class SimulatedClient implements Runnable {
        private final int clientId;
        private final CountDownLatch connectLatch;
        private final CountDownLatch startSignal;
        private final CountDownLatch sendCompleteLatch;

        private Socket socket;
        private BufferedReader reader;
        private PrintWriter writer;
        private volatile boolean running = true;

        public SimulatedClient(int clientId,
                               CountDownLatch connectLatch,
                               CountDownLatch startSignal,
                               CountDownLatch sendCompleteLatch) {
            this.clientId = clientId;
            this.connectLatch = connectLatch;
            this.startSignal = startSignal;
            this.sendCompleteLatch = sendCompleteLatch;
        }

        @Override
        public void run() {
            try {
                socket = new Socket(HOST, PORT);
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);

                Thread readerThread = new Thread(this::readLoop, "client-reader-" + clientId);
                readerThread.setDaemon(true);
                readerThread.start();

                connectLatch.countDown();

                // Wait for synchronized start
                startSignal.await();

                for (int seq = 1; seq <= MESSAGES_PER_CLIENT; seq++) {
                    long now = System.currentTimeMillis();
                    String payload = String.format("[BENCHMARK]|C%d|%d|%d|msg_%d", clientId, seq, now, seq);

                    writer.println(payload);
                    totalMessagesSent.incrementAndGet();
                    totalBytesSent.addAndGet(payload.getBytes(StandardCharsets.UTF_8).length);

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
            try {
                String line;
                while (running && (line = reader.readLine()) != null) {
                    long recvTime = System.currentTimeMillis();
                    totalMessagesReceived.incrementAndGet();
                    totalBytesReceived.addAndGet(line.getBytes(StandardCharsets.UTF_8).length);

                    if (line.startsWith("[BENCHMARK]|")) {
                        String[] parts = line.split("\\|");
                        if (parts.length >= 4) {
                            try {
                                long sentTime = Long.parseLong(parts[3]);
                                long latency = Math.max(0, recvTime - sentTime);
                                latenciesMs.add(latency);
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }
                }
            } catch (IOException e) {
                if (running) {
                    connectionErrors.incrementAndGet();
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
}

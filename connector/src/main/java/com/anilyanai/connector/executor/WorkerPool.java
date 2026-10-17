package com.anilyanai.connector.executor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * =========================================================================================
 * 🚀 WorkerPool (Dedicated Platform Thread Worker Pool)
 * =========================================================================================
 *
 * <p>Uses dedicated OS platform threads with a high-throughput FIFO queue. Eliminates
 * ForkJoinPool work-stealing overhead, carrier unmounting, and internal queue-stealing
 * context switches during intense multi-hundred-thousand packet bursts.</p>
 */
public class WorkerPool {

    private final ExecutorService executor;

    public WorkerPool(int threadCount) {
        this(threadCount, "inbound-worker");
    }

    public WorkerPool(int threadCount, String threadNamePrefix) {
        int threads = Math.max(1, threadCount);
        AtomicInteger threadId = new AtomicInteger(1);
        this.executor = new ThreadPoolExecutor(
                threads,
                threads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                r -> {
                    Thread t = new Thread(r, threadNamePrefix + "-" + threadId.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
        );
    }

    public void submit(Runnable task) {
        executor.submit(task);
    }

    public void shutdown() {
        executor.shutdown();
    }
}

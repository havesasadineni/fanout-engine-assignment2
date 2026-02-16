package com.fanout.engine;

import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class FanOutEngine {
    private final EngineConfig cfg;
    private final Stats stats = new Stats();
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    private final Map<SinkType, BlockingQueue<Task>> queues = new EnumMap<>(SinkType.class);
    private final Map<SinkType, ExecutorService> workers = new EnumMap<>(SinkType.class);
    private final Map<SinkType, RateLimiter> limiters = new EnumMap<>(SinkType.class);
    private final Map<SinkType, DlqWriter> dlqWriters = new EnumMap<>(SinkType.class);
    private final Map<SinkType, MockSink> sinks = new EnumMap<>(SinkType.class);
    private final Map<SinkType, Transformer> transformers = new EnumMap<>(SinkType.class);

    private ScheduledExecutorService statsScheduler;

    // Per item tracking: how many sinks have completed (success or DLQ)
    private final ConcurrentHashMap<Long, LongAdder> doneCount = new ConcurrentHashMap<>();
    private int sinkCount = 0;

    public FanOutEngine(EngineConfig cfg) {
        this.cfg = cfg;
        init();
    }

    public Stats stats() { return stats; }

    private void init() {
        if (cfg.sinks == null || cfg.sinks.isEmpty()) {
            throw new IllegalArgumentException("No sinks configured");
        }
        sinkCount = cfg.sinks.size();

        for (SinkConfig sc : cfg.sinks) {
            SinkType type = sc.type;

            queues.put(type, new ArrayBlockingQueue<>(sc.queueCapacity));
            workers.put(type, Executors.newFixedThreadPool(sc.workers, r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("worker-" + type + "-" + t.getId());
                return t;
            }));
            limiters.put(type, new RateLimiter(sc.rateLimitPerSec));
            dlqWriters.put(type, new DlqWriter(Path.of("dlq-" + type.name().toLowerCase() + ".txt")));
            sinks.put(type, new MockSink(type, sc.failureRate));
            transformers.put(type, Transformers.forSink(type));
        }
    }

    public void start() {
        // start workers
        for (SinkConfig sc : cfg.sinks) {
            SinkType type = sc.type;
            ExecutorService exec = workers.get(type);
            BlockingQueue<Task> q = queues.get(type);

            for (int i = 0; i < sc.workers; i++) {
                exec.submit(() -> workerLoop(type, q));
            }
        }

        // stats printer
        statsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("stats");
            return t;
        });

        final long startMs = System.currentTimeMillis();
        statsScheduler.scheduleAtFixedRate(() -> printStats(startMs), cfg.statsEverySeconds, cfg.statsEverySeconds, TimeUnit.SECONDS);
    }

    public void stopGracefully() {
        stopping.set(true);

        // stop workers
        for (ExecutorService e : workers.values()) {
            e.shutdown();
        }
        for (ExecutorService e : workers.values()) {
            try { e.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }

        // stop stats
        if (statsScheduler != null) {
            statsScheduler.shutdownNow();
        }
    }

    /** Fan-out ingestion: puts the same WorkItem into each sink queue (bounded => backpressure). */
    public void ingest(WorkItem item) throws InterruptedException {
        if (stopping.get()) return;

        stats.ingested.increment();
        doneCount.putIfAbsent(item.id, new LongAdder());

        // enqueue into each sink queue
        for (SinkConfig sc : cfg.sinks) {
            SinkType type = sc.type;
            queues.get(type).put(new Task(item, 0));
        }
    }

    /** Wait until all previously ingested items are completed across all sinks. */
    public void awaitQuiescence(long timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            if (doneCount.isEmpty()) return;
            Thread.sleep(200);
        }
        throw new RuntimeException("Timeout waiting for completion. Remaining items: " + doneCount.size());
    }

    private void workerLoop(SinkType type, BlockingQueue<Task> q) {
        RateLimiter limiter = limiters.get(type);
        MockSink sink = sinks.get(type);
        Transformer transformer = transformers.get(type);
        DlqWriter dlq = dlqWriters.get(type);

        while (!stopping.get() || !q.isEmpty()) {
            Task task;
            try {
                task = q.poll(200, TimeUnit.MILLISECONDS);
                if (task == null) continue;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            limiter.acquire();

            try {
                byte[] payload = transformer.transform(task.item);
                sink.send(payload);

                stats.successBySink.get(type).increment();
                markDone(task.item.id);
            } catch (Exception ex) {
                stats.failBySink.get(type).increment();

                if (task.attempt + 1 < cfg.maxRetries) {
                    // retry with small backoff
                    sleepQuiet(30L * (task.attempt + 1));
                    try {
                        q.put(new Task(task.item, task.attempt + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        // if interrupted, DLQ it
                        dlq.append(task.item.rawLine);
                        stats.dlqBySink.get(type).increment();
                        markDone(task.item.id);
                    }
                } else {
                    // DLQ
                    dlq.append(task.item.rawLine);
                    stats.dlqBySink.get(type).increment();
                    markDone(task.item.id);
                }
            }
        }
    }

    private void markDone(long itemId) {
        LongAdder adder = doneCount.get(itemId);
        if (adder == null) return;
        adder.increment();

        if (adder.sum() >= sinkCount) {
            doneCount.remove(itemId);
            stats.completedAllSinks.increment();
        }
    }

    private void printStats(long startMs) {
        long now = System.currentTimeMillis();
        double seconds = Math.max(1.0, (now - startMs) / 1000.0);
        long ing = stats.ingested.sum();
        long comp = stats.completedAllSinks.sum();
        double rps = ing / seconds;

        System.out.println("=== STATS @" + Instant.ofEpochMilli(now) + " ===");
        System.out.println("Ingested: " + ing + " | FullyDone(all sinks): " + comp + " | Throughput: " + String.format("%.2f", rps) + " rec/s");
        for (SinkType t : SinkType.values()) {
            System.out.println(" - " + t +
                    " ok=" + stats.successBySink.get(t).sum() +
                    " fail=" + stats.failBySink.get(t).sum() +
                    " dlq=" + stats.dlqBySink.get(t).sum());
        }
        System.out.println("==============================");
    }

    private static void sleepQuiet(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }

    private static class Task {
        final WorkItem item;
        final int attempt;

        Task(WorkItem item, int attempt) {
            this.item = item;
            this.attempt = attempt;
        }
    }
}

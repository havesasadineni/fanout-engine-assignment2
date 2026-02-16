package com.fanout.engine;

public class RateLimiter {
    private final long intervalNanos; // nanos per token
    private long nextAllowedTime;

    public RateLimiter(int permitsPerSecond) {
        if (permitsPerSecond <= 0) throw new IllegalArgumentException("permitsPerSecond must be > 0");
        this.intervalNanos = 1_000_000_000L / permitsPerSecond;
        this.nextAllowedTime = System.nanoTime();
    }

    public synchronized void acquire() {
        long now = System.nanoTime();
        if (now < nextAllowedTime) {
            long sleepNanos = nextAllowedTime - now;
            try {
                Thread.sleep(sleepNanos / 1_000_000L, (int)(sleepNanos % 1_000_000L));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            nextAllowedTime += intervalNanos;
        } else {
            nextAllowedTime = now + intervalNanos;
        }
    }
}

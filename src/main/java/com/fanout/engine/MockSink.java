package com.fanout.engine;

import java.util.Random;

public class MockSink {
    private final SinkType type;
    private final double failureRate;
    private final Random rnd = new Random();

    public MockSink(SinkType type, double failureRate) {
        this.type = type;
        this.failureRate = failureRate;
    }

    public void send(byte[] payload) {
        // simulate network/IO cost
        try { Thread.sleep(2); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }

        // simulate occasional failures
        if (rnd.nextDouble() < failureRate) {
            throw new RuntimeException(type + " simulated failure");
        }
    }
}

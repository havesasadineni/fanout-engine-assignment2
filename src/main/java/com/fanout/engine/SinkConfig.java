package com.fanout.engine;

public class SinkConfig {
    public SinkType type;
    public int workers = 2;
    public int rateLimitPerSec = 50;
    public int queueCapacity = 5000;
    public double failureRate = 0.01; // mock failure probability (0..1)

    public SinkConfig() {}
}

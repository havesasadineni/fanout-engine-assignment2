package com.fanout.engine;

import java.util.ArrayList;
import java.util.List;

public class EngineConfig {
    public String inputPath = "sample.jsonl";
    public int maxRetries = 3;
    public int statsEverySeconds = 5;
    public List<SinkConfig> sinks = new ArrayList<>();

    public EngineConfig() {}
}

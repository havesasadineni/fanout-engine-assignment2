package com.fanout.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class FanOutEngineTest {
    private static final ObjectMapper M = new ObjectMapper();

    @Test
    void smallRunCompletes() throws Exception {
        // create tiny input
        Path input = Path.of("test-sample.jsonl");
        Files.write(input, List.of(
                "{\"name\":\"a\",\"v\":1}",
                "{\"name\":\"b\",\"v\":2}",
                "{\"name\":\"c\",\"v\":3}"
        ));

        EngineConfig cfg = new EngineConfig();
        cfg.inputPath = input.toString();
        cfg.maxRetries = 3;
        cfg.statsEverySeconds = 1;

        cfg.sinks = List.of(
                mkSink(SinkType.REST, 2, 1000, 100, 0.0),
                mkSink(SinkType.GRPC, 2, 1000, 100, 0.0),
                mkSink(SinkType.MQ, 2, 1000, 100, 0.0),
                mkSink(SinkType.WIDECOLUMN, 2, 1000, 100, 0.0)
        );

        FanOutEngine engine = new FanOutEngine(cfg);
        engine.start();

        long id = 0;
        for (String line : Files.readAllLines(input)) {
            engine.ingest(new WorkItem(++id, line, M.readTree(line)));
        }

        engine.awaitQuiescence(10);
        engine.stopGracefully();

        assertTrue(engine.stats().completedAllSinks.sum() >= 3);
    }

    private static SinkConfig mkSink(SinkType type, int workers, int rate, int cap, double failRate) {
        SinkConfig s = new SinkConfig();
        s.type = type;
        s.workers = workers;
        s.rateLimitPerSec = rate;
        s.queueCapacity = cap;
        s.failureRate = failRate;
        return s;
    }
}

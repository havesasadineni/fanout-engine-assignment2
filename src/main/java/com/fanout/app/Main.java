package com.fanout.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fanout.engine.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;

public class Main {
    private static final ObjectMapper M = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : "config.json";

        EngineConfig cfg = M.readValue(Path.of(configPath).toFile(), EngineConfig.class);

        System.out.println("=== FanOut Engine ===");
        System.out.println("Config: " + configPath);
        System.out.println("Input : " + cfg.inputPath);
        System.out.println("=====================");

        if (!Files.exists(Path.of(cfg.inputPath))) {
            System.err.println("Input file not found: " + cfg.inputPath);
            System.err.println("Create sample.jsonl and config.json (I provided samples below).");
            System.exit(1);
        }

        FanOutEngine engine = new FanOutEngine(cfg);
        engine.start();

        long id = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(cfg.inputPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                JsonNode node = M.readTree(line);
                WorkItem item = new WorkItem(++id, line, node);

                // backpressure happens here if any sink queue is full
                engine.ingest(item);
            }
        }

        // wait for all queued work to finish
        engine.awaitQuiescence(120);
        engine.stopGracefully();

        System.out.println("DONE. Ingested=" + engine.stats().ingested.sum()
                + " FullyDone=" + engine.stats().completedAllSinks.sum());
    }
}

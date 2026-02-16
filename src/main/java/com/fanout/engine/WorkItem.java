package com.fanout.engine;

import com.fasterxml.jackson.databind.JsonNode;

public class WorkItem {
    public final long id;
    public final String rawLine;      // original line for DLQ
    public final JsonNode record;     // parsed JSON
    public final long createdAtMs;

    public WorkItem(long id, String rawLine, JsonNode record) {
        this.id = id;
        this.rawLine = rawLine;
        this.record = record;
        this.createdAtMs = System.currentTimeMillis();
    }
}

\# Assignment 2 — High-Throughput Fan-Out Engine (Java)



\## Overview

This project implements a high-throughput \*\*fan-out + transformation engine\*\* in Java.

It \*\*streams\*\* a large input file (JSONL, line-by-line), transforms each record per sink, and \*\*fan-outs\*\* the record to multiple sinks concurrently.



Key features:

\- Streaming ingestion (does not load file into memory)

\- Fan-out to 4 sinks in parallel (mock REST, gRPC, MQ, WideColumn)

\- Per-sink bounded queue (\*\*backpressure\*\*)

\- Per-sink \*\*rate limiting\*\*

\- \*\*Retry\*\* up to `maxRetries` (default 3)

\- \*\*DLQ\*\* file per sink when retries are exhausted

\- \*\*Stats every N seconds\*\*: throughput + success/fail/dlq counters per sink

\- Graceful shutdown after all queued work finishes



\## Architecture (high level)

Producer (file reader) → fan-out → per-sink bounded queue → worker threads → transformer → mock sink send  

\- Backpressure: if any sink queue is full, ingestion blocks on `queue.put()`.

\- Concurrency: each sink has its own worker thread pool.

\- Reliability: failures are retried; after max retries, the raw record is written to that sink’s DLQ file.



\## Input Format

This engine reads \*\*JSONL\*\*: one valid JSON object per line.

## Screenshots

![Run Output](Screenshot%20(154).png)


Example (`sample.jsonl`):

```json

{"user":"u1","action":"login","ts":1700000001}

{"user":"u2","action":"buy","ts":1700000002,"amount":19.99}




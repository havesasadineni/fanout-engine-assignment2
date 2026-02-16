package com.fanout.engine;

public interface Transformer {
    byte[] transform(WorkItem item) throws Exception;
}

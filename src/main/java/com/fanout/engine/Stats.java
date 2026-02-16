package com.fanout.engine;

import java.util.EnumMap;
import java.util.concurrent.atomic.LongAdder;

public class Stats {
    public final LongAdder ingested = new LongAdder();
    public final LongAdder completedAllSinks = new LongAdder();

    public final EnumMap<SinkType, LongAdder> successBySink = new EnumMap<>(SinkType.class);
    public final EnumMap<SinkType, LongAdder> failBySink = new EnumMap<>(SinkType.class);
    public final EnumMap<SinkType, LongAdder> dlqBySink = new EnumMap<>(SinkType.class);

    public Stats() {
        for (SinkType t : SinkType.values()) {
            successBySink.put(t, new LongAdder());
            failBySink.put(t, new LongAdder());
            dlqBySink.put(t, new LongAdder());
        }
    }
}

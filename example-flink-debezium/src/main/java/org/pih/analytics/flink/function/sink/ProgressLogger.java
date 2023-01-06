package org.pih.analytics.flink.function.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class ProgressLogger implements SinkFunction<ChangeEvent> {

    private final long startTime = System.currentTimeMillis();
    private final Map<String, Integer> tableCounts = new HashMap<>();
    private final long delayMillis;
    private long lastLoggedTime = System.currentTimeMillis();

    public ProgressLogger(long delayMillis) {
        this.delayMillis = delayMillis;
    }

    @Override
    public void invoke(ChangeEvent value, Context context) throws Exception {
        long currentTime = System.currentTimeMillis();
        tableCounts.merge(value.getTable(), 1, Integer::sum);
        if ((lastLoggedTime+delayMillis) < currentTime) {
            log(currentTime);
        }
    }

    public void log(long currentTime) {
        System.out.println(Duration.ofMillis(currentTime-startTime) + ": " + tableCounts);
        lastLoggedTime = currentTime;
    }
}
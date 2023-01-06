package org.pih.analytics.flink.function.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

import java.util.HashMap;
import java.util.Map;

public class ProgressLogger implements SinkFunction<ChangeEvent> {

    private final long startTime = System.currentTimeMillis();
    private final Map<String, Integer> tableCounts = new HashMap<>();
    private int lastLoggedDuration = 0;

    @Override
    public void invoke(ChangeEvent value, Context context) throws Exception {
        long currentTime = System.currentTimeMillis();
        int minutes = (int)((currentTime - startTime) / 60000);
        tableCounts.merge(value.getTable(), 1, Integer::sum);
        if (lastLoggedDuration != minutes) {
            lastLoggedDuration = minutes;
            System.out.println(minutes + "m: " + tableCounts);
        }
    }
}
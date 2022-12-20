package org.pih.analytics.flink.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.pih.analytics.flink.debezium.DebeziumEvent;

import java.util.HashMap;
import java.util.Map;

public class TableOperationCountAggregator implements AggregateFunction<DebeziumEvent, Map<String, Integer>, Map<String, Integer>> {

    @Override
    public Map<String, Integer> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<String, Integer> add(DebeziumEvent event, Map<String, Integer> aggregator) {
        Integer existing = aggregator.get(getKey(event));
        aggregator.put(getKey(event), (existing == null ? 1 : existing + 1));
        return aggregator;
    }

    @Override
    public Map<String, Integer> merge(Map<String, Integer> aggregator1, Map<String, Integer> aggregator2) {
        Map<String, Integer> m = new HashMap<>(aggregator1);
        for (String o : aggregator2.keySet()) {
            Integer existing = m.get(o);
            Integer newCount = aggregator2.get(o) + (existing == null ? 0 : existing);
            m.put(o, newCount);
        }
        return m;
    }

    @Override
    public Map<String, Integer> getResult(Map<String, Integer> aggregator) {
        return aggregator;
    }

    public String getKey(DebeziumEvent event) {
        return event.getTable() + " - " + event.getOperation().name();
    }
}

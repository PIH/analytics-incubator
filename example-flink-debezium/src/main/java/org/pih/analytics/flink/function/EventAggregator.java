package org.pih.analytics.flink.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.pih.analytics.flink.debezium.DebeziumEvent;

import java.util.ArrayList;
import java.util.List;

public class EventAggregator implements AggregateFunction<DebeziumEvent, List<DebeziumEvent>, List<DebeziumEvent>> {
    @Override
    public List<DebeziumEvent> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<DebeziumEvent> add(DebeziumEvent DebeziumEvent, List<DebeziumEvent> events) {
        events.add(DebeziumEvent);
        return events;
    }

    @Override
    public List<DebeziumEvent> getResult(List<DebeziumEvent> events) {
        return null;
    }

    @Override
    public List<DebeziumEvent> merge(List<DebeziumEvent> DebeziumEvents, List<DebeziumEvent> acc1) {
        acc1.addAll(DebeziumEvents);
        return acc1;
    }
}

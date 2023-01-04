package org.pih.analytics.flink.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

import java.util.ArrayList;
import java.util.List;

public class EventAggregator implements AggregateFunction<ChangeEvent, List<ChangeEvent>, List<ChangeEvent>> {
    @Override
    public List<ChangeEvent> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<ChangeEvent> add(ChangeEvent ChangeEvent, List<ChangeEvent> events) {
        events.add(ChangeEvent);
        return events;
    }

    @Override
    public List<ChangeEvent> getResult(List<ChangeEvent> events) {
        return null;
    }

    @Override
    public List<ChangeEvent> merge(List<ChangeEvent> ChangeEvents, List<ChangeEvent> acc1) {
        acc1.addAll(ChangeEvents);
        return acc1;
    }
}

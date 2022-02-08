package org.pih.analytics.flink.experimental;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.pih.analytics.flink.Event;

import java.util.ArrayList;
import java.util.List;

public class EventAggregator implements AggregateFunction<Event, List<Event>, List<Event>> {
    @Override
    public List<Event> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Event> add(Event event, List<Event> events) {
        events.add(event);
        return events;
    }

    @Override
    public List<Event> getResult(List<Event> events) {
        return null;
    }

    @Override
    public List<Event> merge(List<Event> events, List<Event> acc1) {
        acc1.addAll(events);
        return acc1;
    }
}

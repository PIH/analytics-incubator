package org.pih.analytics.flink.function.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

/**
 * Takes two events and returns the event with the more recent timestamp
 */
public class LatestEventReducer implements ReduceFunction<ChangeEvent> {

    @Override
    public ChangeEvent reduce(ChangeEvent e1, ChangeEvent e2) {
        return e1.getTimestamp() > e2.getTimestamp() ? e1 : e2;
    }
}

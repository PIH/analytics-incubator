package org.pih.analytics.flink.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.pih.analytics.flink.debezium.DebeziumEvent;
import org.pih.analytics.flink.debezium.DebeziumOperation;
import org.pih.analytics.flink.util.ObjectMap;

/**
 * Filter that removes any READ events that either have no "after" value, or whose "after" value is voided
 */
public class DeletedOnReadFilter implements FilterFunction<DebeziumEvent> {

    @Override
    public boolean filter(DebeziumEvent debeziumEvent) {
        if (debeziumEvent.getOperation() == DebeziumOperation.READ) {
            ObjectMap after = debeziumEvent.getAfter();
            return after == null || !after.getBoolean("voided", false);
        }
        return true;
    }
}

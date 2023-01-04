package org.pih.analytics.flink.function.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.debezium.DebeziumOperation;
import org.pih.analytics.flink.util.ObjectMap;

/**
 * Filter that removes any READ events that either have no "after" value, or whose "after" value is voided
 */
public class DeletedOnReadFilter implements FilterFunction<ChangeEvent> {

    @Override
    public boolean filter(ChangeEvent ChangeEvent) {
        if (ChangeEvent.getOperation() == DebeziumOperation.READ) {
            ObjectMap after = ChangeEvent.getAfter();
            return after == null || !after.getBoolean("voided", false);
        }
        return true;
    }
}

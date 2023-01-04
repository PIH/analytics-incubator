package org.pih.analytics.flink.function.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

/**
 * Events that are associated directly with patient
 */
public class TableFilter implements FilterFunction<ChangeEvent> {

    private String tableName;

    public TableFilter(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public boolean filter(ChangeEvent event) {
        return event.getTable().equals(tableName);
    }
}

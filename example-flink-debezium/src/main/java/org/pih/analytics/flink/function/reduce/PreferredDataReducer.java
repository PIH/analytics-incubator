package org.pih.analytics.flink.function.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

/**
 * This reducer returns the more preferred data based on voided, preferred, and date_created (more recent preferred)
 */
public class PreferredDataReducer implements ReduceFunction<ChangeEvent> {

    @Override
    public ChangeEvent reduce(ChangeEvent event1, ChangeEvent event2) {
        if (event1.isDeleted()) {
            return event2;
        }
        if (event2.isDeleted()) {
            return event1;
        }
        if (isPreferred(event1) && !isPreferred(event2)) {
            return event1;
        }
        if (!isPreferred(event1) && isPreferred(event2)) {
            return event2;
        }
        return event1.getDateCreated().after(event2.getDateCreated()) ? event1 : event2;
    }

    protected boolean isPreferred(ChangeEvent event) {
        return event.getValues().getBoolean("preferred", false);
    }
}

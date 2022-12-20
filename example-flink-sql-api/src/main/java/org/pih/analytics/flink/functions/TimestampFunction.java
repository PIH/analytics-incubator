package org.pih.analytics.flink.functions;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class TimestampFunction extends ScalarFunction {

    public Timestamp eval(Long epochMillis) {
        if (epochMillis == null) {
            return null;
        }
        return new Timestamp(epochMillis);
    }

}

package org.pih.analytics.flink.functions;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Date;

public class DateFunction extends ScalarFunction {

    public Date eval(Long epochDays) {
        if (epochDays == null) {
            return null;
        }
        long millis = epochDays * 24 * 60 * 60 * 1000;
        return new Date(millis);
    }

}

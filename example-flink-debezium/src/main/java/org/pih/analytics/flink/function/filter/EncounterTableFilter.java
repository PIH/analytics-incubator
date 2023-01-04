package org.pih.analytics.flink.function.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Events that are associated directly with patient
 */
public class EncounterTableFilter implements FilterFunction<ChangeEvent> {

    public static final Set<String> encounterTables = new HashSet<>(Arrays.asList(
            "encounter", "obs"
    ));

    @Override
    public boolean filter(ChangeEvent event) {
        String tableName = event.getTable();
        if (encounterTables.contains(tableName)) {
            if (tableName.equals("obs")) {
                return event.getEncounterId() != null;
            }
            return true;
        }
        return false;
    }
}

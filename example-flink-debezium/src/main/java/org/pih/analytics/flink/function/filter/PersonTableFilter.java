package org.pih.analytics.flink.function.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Events that are associated directly with person
 */
public class PersonTableFilter implements FilterFunction<ChangeEvent> {

    public static final Set<String> personTables = new HashSet<>(Arrays.asList(
            "person", "person_name", "person_address", "person_attribute", "obs"
    ));

    @Override
    public boolean filter(ChangeEvent event) {
        String tableName = event.getTable();
        if (personTables.contains(tableName)) {
            if (tableName.equals("obs")) {
                return event.getEncounterId() == null;
            }
            return true;
        }
        return false;
    }
}

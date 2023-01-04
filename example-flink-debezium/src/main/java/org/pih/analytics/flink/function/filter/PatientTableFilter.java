package org.pih.analytics.flink.function.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Events that are associated directly with patient
 */
public class PatientTableFilter implements FilterFunction<ChangeEvent> {

    public static final Set<String> patientTables = new HashSet<>(Arrays.asList(
            "patient", "patient_identifier"
    ));

    @Override
    public boolean filter(ChangeEvent event) {
        String tableName = event.getTable();
        if (patientTables.contains(tableName)) {
            return true;
        }
        return false;
    }
}

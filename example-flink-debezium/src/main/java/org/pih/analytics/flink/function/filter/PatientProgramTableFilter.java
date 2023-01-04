package org.pih.analytics.flink.function.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Events that are associated directly with patient program
 */
public class PatientProgramTableFilter implements FilterFunction<ChangeEvent> {

    public static final Set<String> patientProgramTables = new HashSet<>(Arrays.asList(
            "patient_program", "patient_state", "patient_program_attribute"
    ));

    @Override
    public boolean filter(ChangeEvent event) {
        String tableName = event.getTable();
        if (patientProgramTables.contains(tableName)) {
            return true;
        }
        return false;
    }
}

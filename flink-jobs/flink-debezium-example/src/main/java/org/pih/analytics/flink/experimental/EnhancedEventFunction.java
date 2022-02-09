package org.pih.analytics.flink.experimental;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.pih.analytics.flink.Event;
import org.pih.analytics.flink.util.Mysql;

import java.sql.Connection;

/**
 * The purpose of this function is to take in an Event, and enhance that Event with additional elements necessary for further processing
 * The primary use case is to add keys that are available only via a join in the source data model.  For example, in order to key
 * patient_state directly to patient_id, one needs to query the patient_program table which links these together.
 */
public class EnhancedEventFunction extends KeyedProcessFunction<String, Event, Event> {

    private transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = Mysql.createConnection();
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void processElement(Event event, KeyedProcessFunction<String, Event, Event>.Context context, Collector<Event> collector) throws Exception {
        enhancePatientState(event);
        collector.collect(event);
    }

    protected void enhancePatientState(Event event) {
        if (event.table.equals("patient_state")) {
            Integer patientProgramId = event.values.getInteger("patient_program_id");
            Object patientId = Mysql.lookupValue(connection, "patient_program", "patient_id", "patient_program_id", patientProgramId);
            event.values.put("patient_id", patientId);
        }
    }
}
package org.pih.analytics.flink.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.pih.analytics.flink.config.MysqlDataSource;
import org.pih.analytics.flink.debezium.DebeziumEvent;

import java.sql.Connection;

/**
 * The purpose of this function is to take in a DebeziumEvent, and enhance that Event with additional elements necessary for further processing
 * The primary use case is to add keys that are available only via a join in the source data model.  For example, in order to key
 * patient_state directly to patient_id, one needs to query the patient_program table which links these together.
 */
public class EnhancedEventFunction extends KeyedProcessFunction<String, DebeziumEvent, DebeziumEvent> {

    private transient Connection connection;
    private final MysqlDataSource dataSource;

    public EnhancedEventFunction(MysqlDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = dataSource.openConnection();
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void processElement(DebeziumEvent event, KeyedProcessFunction<String, DebeziumEvent, DebeziumEvent>.Context context, Collector<DebeziumEvent> collector) throws Exception {
        enhancePatientState(event);
        enhancePersonAndPatientIds(event);
        collector.collect(event);
    }

    protected void enhancePatientState(DebeziumEvent event) {
        if (event.getTable().equals("patient_state")) {
            Integer patientProgramId = event.getValues().getInteger("patient_program_id");
            Object patientId = MysqlDataSource.lookupValue(connection, "patient_program", "patient_id", "patient_program_id", patientProgramId);
            event.addValue("patient_id", patientId);
        }
    }

    protected void enhancePersonAndPatientIds(DebeziumEvent event) {
        Integer patientId = event.getValues().getInteger("patient_id");
        Integer personId = event.getValues().getInteger("person_id");
        if (patientId == null && personId != null) {
            event.addValue("patient_id", personId);
        }
        else if (patientId != null && personId == null) {
            event.addValue("person_id", patientId);
        }
    }
}
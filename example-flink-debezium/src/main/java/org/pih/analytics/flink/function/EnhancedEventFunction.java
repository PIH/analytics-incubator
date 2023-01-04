package org.pih.analytics.flink.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.pih.analytics.flink.config.MysqlDataSource;
import org.pih.analytics.flink.debezium.ChangeEvent;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * The purpose of this function is to take in a ChangeEvent, and enhance that Event with additional elements necessary for further processing
 * The primary use case is to add keys that are available only via a join in the source data model.  For example, in order to key
 * patient_state directly to patient_id, one needs to query the patient_program table which links these together.
 */
public class EnhancedEventFunction extends KeyedProcessFunction<String, ChangeEvent, ChangeEvent> {

    private static String lastTableName;
    private static Map<String, Integer> tableCount = new HashMap<>();
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
    public void processElement(ChangeEvent event, KeyedProcessFunction<String, ChangeEvent, ChangeEvent>.Context context, Collector<ChangeEvent> collector) throws Exception {
        Integer count = tableCount.getOrDefault(event.getTable(), 0) + 1;
        tableCount.put(event.getTable(), count);
        if ((lastTableName != null && !lastTableName.equals(event.getTable())) || count % 100000 == 0) {
            System.out.println(count + ": " + tableCount);
        }
        lastTableName = event.getTable();

        // Add person_id and patient_id as needed
        enhancePersonAndPatientData(event);
        // Add patient_id to program events
        addIfMissing(event, "patient_id", "patient_program", "patient_program_id");
        // Add patient_id to visit events
        addIfMissing(event, "patient_id", "visit", "visit_id");
        // Add patient_id and encounter_id to order events
        addIfMissing(event, "patient_id", "orders", "order_id");
        addIfMissing(event, "encounter_id", "orders", "order_id");
        // Add patient_id to allergy events
        addIfMissing(event, "patient_id", "allergy", "allergy_id");
        // TODO: Relationship events (person_a, person_b)
        collector.collect(event);
    }

    protected void enhancePersonAndPatientData(ChangeEvent event) {
        Integer patientId = event.getValues().getInteger("patient_id");
        Integer personId = event.getValues().getInteger("person_id");
        if (patientId == null && personId != null) {
            event.addValue("patient_id", personId);
        }
        else if (patientId != null && personId == null) {
            event.addValue("person_id", patientId);
        }
    }

    /**
     * If the given event does not have the columnNeeded, but does have the joinColumn
     * this looks up the value in MySQL in the joinTable with same columnNeeded and joinColumn names
     */
    protected void addIfMissing(ChangeEvent event, String columnNeeded, String joinTable, String joinColumn) {
        Object val = event.getValues().get(columnNeeded);
        Object joinValue = event.getValues().get(joinColumn);
        if (val == null && joinValue != null) {
            val = MysqlDataSource.lookupValue(connection, joinTable, columnNeeded, joinColumn, joinValue);
            event.addValue(columnNeeded, val);
        }
    }
}
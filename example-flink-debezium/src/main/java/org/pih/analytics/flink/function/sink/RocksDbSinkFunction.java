package org.pih.analytics.flink.function.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.model.Person;
import org.pih.analytics.flink.util.Rocks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RocksDbSinkFunction extends RichSinkFunction<ChangeEvent> {

    private static final Set<Integer> patientIds = new HashSet<>();

    private final String name;
    private Rocks patientDb;
    private long lastLogging = 0;

    public static final List<String> tables = Arrays.asList(
            "person", "person_name", "person_address", "person_attribute"
    );

    public RocksDbSinkFunction(String name) {
        this.name = name;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        patientDb = new Rocks(name);
    }

    @Override
    public void close() throws Exception {
        try {
            patientDb.close();
        }
        catch (Exception e) {
        }
    }

    @Override
    public void invoke(ChangeEvent value, Context context) throws Exception {
        if (value.getPatientId() != null) {
            patientIds.add(value.getPatientId());
        }
        if (value.getPersonOrPatientId() != null) {
            Person person = patientDb.get(value.getPersonOrPatientId());
            if (person == null) {
                person = new Person();
                person.setPersonId(value.getPersonOrPatientId());
            }
            if (tables.contains(value.getTable())) {
                person.addEvent(value);
            }
            patientDb.put(value.getPersonOrPatientId(), person);
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime-lastLogging > 5000) {
            lastLogging = currentTime;
            System.out.println("Patients Added: " + patientIds.size());
        }
    }
}
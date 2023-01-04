package org.pih.analytics.flink.function.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.mapdb.DB;
import org.pih.analytics.flink.debezium.ChangeEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MapDbSinkFunction extends RichSinkFunction<ChangeEvent> {

    private DB db;
    private ConcurrentMap patientMap = new ConcurrentHashMap();
    private ConcurrentMap patientProgramMap = new ConcurrentHashMap();
    private long lastLogging = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        //db = DBMaker.fileDB("/tmp/flinktesting/testing.db").make();
        //patientMap = db.hashMap("patients").createOrOpen();
        //patientProgramMap = db.hashMap("patientprograms").createOrOpen();
    }

    @Override
    public void invoke(ChangeEvent value, Context context) throws Exception {
        if (value.getPersonOrPatientId() != null) {
            //System.out.println("patientMap: " + value.getPersonOrPatientId() + "->" + value);
            List<ChangeEvent> events = (List<ChangeEvent>)patientMap.get(value.getPersonOrPatientId());
            if (events == null) {
                events = new ArrayList<>();
                patientMap.put(value.getPersonOrPatientId(), events);
            }
            events.add(value);
        }
        if (value.getPatientProgramId() != null) {
            //System.out.println("patientProgramMap: " + value.getPatientProgramId() + "->" + value);
            List<ChangeEvent> events = (List<ChangeEvent>)patientProgramMap.get(value.getPatientProgramId());
            if (events == null) {
                events = new ArrayList<>();
                patientProgramMap.put(value.getPatientProgramId(), events);
            }
            events.add(value);
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime-lastLogging > 5000) {
            lastLogging = currentTime;
            System.out.println("Patients: " + getPatientMap().size());
            System.out.println("Patient programs: " + getPatientProgramMap().size());
        }
    }

    public ConcurrentMap getPatientMap() {
        return patientMap;
    }

    public ConcurrentMap getPatientProgramMap() {
        return patientProgramMap;
    }

    @Override
    public void close() throws Exception {
        //db.close();
    }
}